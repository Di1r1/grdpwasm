// Package audin implements the Audio Input Redirection Virtual Channel
// Extension (MS-RDPEAI) for client-to-server microphone redirection.
//
// The live path is the dynamic virtual channel "AUDIO_INPUT" (audin v2,
// like FreeRDP): static GCC names are capped at 8 bytes, so "AUDIO_INPUT"
// cannot be announced there — the Handler still implements the static
// interface for symmetry with rdpsnd, but servers use DVC.
// Only PCM 16-bit formats are offered to the server; the browser captures
// audio via getUserMedia and pushes raw PCM frames through PushPCM.
//
// Message flow (mirrors FreeRDP channels/audin/client/audin_main.c):
//
//	S→C VERSION | C→S VERSION
//	S→C FORMATS | C→S DATA_INCOMING + FORMATS (filtered to PCM16)
//	S→C OPEN    | C→S FORMATCHANGE + OPEN_REPLY
//	C→S DATA_INCOMING + DATA (per captured chunk)
//	S→C FORMATCHANGE (re-open with another agreed format)
package audin

import (
	"encoding/binary"
	"fmt"
	"log/slog"
	"sync"

	"github.com/nakagami/grdp/core"
	"github.com/nakagami/grdp/plugin"
)

const (
	ChannelName   = "AUDIO_INPUT"
	DvcName       = "AUDIO_INPUT"
	ChannelOption = plugin.CHANNEL_OPTION_INITIALIZED |
		plugin.CHANNEL_OPTION_ENCRYPT_RDP
)

// DvcNames lists the DVC names we are willing to use for audio input.
// MS-RDPEAI specifies "AUDIO_INPUT"; the other spellings are tried as
// candidates because Windows 10/11 clients appear to use an explicit
// request whose naming is not documented.
var DvcNames = []string{
	"AUDIO_INPUT",
	"Microsoft::Windows::RDS::AudioInput",
	"AUDIO_INPUT_DVC",
	"audin",
}

// SNDIN PDU types (MS-RDPEAI 2.2).
const (
	MSG_SNDIN_VERSION      = 0x01
	MSG_SNDIN_FORMATS      = 0x02
	MSG_SNDIN_OPEN         = 0x03
	MSG_SNDIN_OPEN_REPLY   = 0x04
	MSG_SNDIN_DATA_INCOM   = 0x05
	MSG_SNDIN_DATA         = 0x06
	MSG_SNDIN_FORMATCHANGE = 0x07
)

// SNDIN_VERSION is the audin protocol version we speak (matches FreeRDP).
const SNDIN_VERSION = 0x02

// WAVE_FORMAT_PCM is the only encoding we capture.
const WAVE_FORMAT_PCM = 0x0001

// MicFormat is the capture format agreed with the server.
type MicFormat struct {
	SamplesPerSec   uint32
	Channels        uint16
	FramesPerPacket uint32
}

// AudioFormat represents a WAVEFORMATEX structure.
type AudioFormat struct {
	Tag            uint16
	Channels       uint16
	SamplesPerSec  uint32
	AvgBytesPerSec uint32
	BlockAlign     uint16
	BitsPerSample  uint16
	ExtraData      []byte
}

func (f AudioFormat) pack() []byte {
	b := make([]byte, 18+len(f.ExtraData))
	binary.LittleEndian.PutUint16(b[0:], f.Tag)
	binary.LittleEndian.PutUint16(b[2:], f.Channels)
	binary.LittleEndian.PutUint32(b[4:], f.SamplesPerSec)
	binary.LittleEndian.PutUint32(b[8:], f.AvgBytesPerSec)
	binary.LittleEndian.PutUint16(b[12:], f.BlockAlign)
	binary.LittleEndian.PutUint16(b[14:], f.BitsPerSample)
	binary.LittleEndian.PutUint16(b[16:], uint16(len(f.ExtraData)))
	copy(b[18:], f.ExtraData)
	return b
}

func unpackAudioFormat(data []byte, offset int) (AudioFormat, int) {
	if len(data)-offset < 18 {
		return AudioFormat{}, offset
	}
	f := AudioFormat{
		Tag:            binary.LittleEndian.Uint16(data[offset:]),
		Channels:       binary.LittleEndian.Uint16(data[offset+2:]),
		SamplesPerSec:  binary.LittleEndian.Uint32(data[offset+4:]),
		AvgBytesPerSec: binary.LittleEndian.Uint32(data[offset+8:]),
		BlockAlign:     binary.LittleEndian.Uint16(data[offset+12:]),
		BitsPerSample:  binary.LittleEndian.Uint16(data[offset+14:]),
	}
	cbSize := int(binary.LittleEndian.Uint16(data[offset+16:]))
	if cbSize > 0 && offset+18+cbSize <= len(data) {
		f.ExtraData = make([]byte, cbSize)
		copy(f.ExtraData, data[offset+18:offset+18+cbSize])
	}
	return f, offset + 18 + cbSize
}

// Handler implements MS-RDPEAI over a static virtual channel.
// It also serves as the DVC audio-input handler via ProcessData.
type Handler struct {
	mu            sync.Mutex
	channelSender core.ChannelSender

	formats []AudioFormat // agreed formats (subset offered to server)
	current int           // index into formats, -1 when closed
	enabled bool          // mic toggle from the UI

	// DVC send callback for the current message's channel
	dvcSendFunc func([]byte)

	// viaDvc tracks whether the current message arrived via DVC
	viaDvc bool

	// onOpen is called when the server opens capture (and on format
	// change) with the agreed capture format. The application should
	// (re)start pushing PCM16 frames via PushPCM.
	onOpen func(MicFormat)

	// onClose is called when the channel goes away.
	onClose func()

	// onProgress reports handshake milestones for UI diagnostics
	// (e.g. "dvc", "version", "formats", "open", "data").
	onProgress func(string)

	sawDVC   bool
	sentData bool

	// Нарезка PCM под FramesPerPacket из OPEN-пакета (как у mstsc:
	// обычно ~10 мс на пакет). 0/невалидно — отправляем как есть.
	framesPerPacket uint32
	bytesPerFrame   int
	pcmBuf          []byte
}

// SetProgressCallback sets a diagnostic callback for handshake milestones.
func (h *Handler) SetProgressCallback(f func(string)) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.onProgress = f
}

func (h *Handler) progress(stage string) {
	h.mu.Lock()
	cb := h.onProgress
	h.mu.Unlock()
	if cb != nil {
		cb(stage)
	}
}

// NewHandler creates a new audin handler. Capture stays disabled until
// SetEnabled(true); the UI mic toggle drives it.
func NewHandler() *Handler {
	return &Handler{current: -1}
}

// SetOpenCallback sets the function called on server OPEN / FORMATCHANGE.
func (h *Handler) SetOpenCallback(f func(MicFormat)) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.onOpen = f
}

// SetCloseCallback sets the function called when capture ends.
func (h *Handler) SetCloseCallback(f func()) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.onClose = f
}

// SetEnabled arms (true) or disarms (false) microphone capture.
// Disarmed: PushPCM drops frames. Server OPEN is ALWAYS accepted so a
// later arming takes effect without reconnect (some servers, e.g. xrdp,
// open capture eagerly at session start and never retry).
func (h *Handler) SetEnabled(b bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.enabled = b
}

// Opened reports whether the server currently holds capture open.
func (h *Handler) Opened() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.current >= 0
}

// PushPCM sends one raw PCM16 (little-endian, interleaved) chunk to the
// server. Each chunk is preceded by a DATA_INCOMING PDU, as required by
// MS-RDPEAI. Frames are silently dropped unless capture is enabled and
// the server holds the channel open.
func (h *Handler) PushPCM(data []byte) {
	h.mu.Lock()
	if !h.enabled || h.current < 0 || len(data) == 0 {
		h.mu.Unlock()
		return
	}
	chunkBytes := 0
	if h.framesPerPacket > 0 && h.bytesPerFrame > 0 {
		chunkBytes = int(h.framesPerPacket) * h.bytesPerFrame
	}
	if chunkBytes <= 0 {
		h.mu.Unlock()
		h.sendPCM(data)
		return
	}

	h.pcmBuf = append(h.pcmBuf, data...)
	var outs [][]byte
	for len(h.pcmBuf) >= chunkBytes {
		part := make([]byte, chunkBytes)
		copy(part, h.pcmBuf[:chunkBytes])
		outs = append(outs, part)
		h.pcmBuf = h.pcmBuf[chunkBytes:]
	}
	// Защита от роста буфера при рассинхроне: держим не больше 4 пакетов.
	if len(h.pcmBuf) > chunkBytes*4 {
		h.pcmBuf = h.pcmBuf[len(h.pcmBuf)-chunkBytes*4:]
	}
	h.mu.Unlock()

	for _, part := range outs {
		h.sendPCM(part)
	}
}

// sendPCM отправляет один PCM-блок: DATA_INCOMING + DATA (MS-RDPEAI).
func (h *Handler) sendPCM(data []byte) {
	if len(data) == 0 {
		return
	}
	if err := h.send([]byte{MSG_SNDIN_DATA_INCOM}); err != nil {
		return
	}
	buf := make([]byte, 1+len(data))
	buf[0] = MSG_SNDIN_DATA
	copy(buf[1:], data)
	_ = h.send(buf)

	h.mu.Lock()
	first := !h.sentData
	h.sentData = true
	h.mu.Unlock()
	if first {
		h.progress("data")
	}
}

// --- plugin.ChannelTransport interface ---

func (h *Handler) GetType() (string, uint32) {
	return ChannelName, ChannelOption
}

func (h *Handler) Sender(s core.ChannelSender) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.channelSender = s
}

// Process handles data from the static virtual channel (already reassembled).
func (h *Handler) Process(s []byte) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("audin: panic in Process", "err", r)
		}
	}()
	h.mu.Lock()
	h.viaDvc = false
	h.mu.Unlock()
	h.ProcessData(s)
}

// ProcessData processes a reassembled audin PDU payload.
// This is used by both the static VChannel path and the DVC path.
func (h *Handler) ProcessData(data []byte) {
	if len(data) < 1 {
		return
	}
	switch data[0] {
	case MSG_SNDIN_VERSION:
		h.progress("version")
		h.processVersion(data[1:])
	case MSG_SNDIN_FORMATS:
		h.progress("formats")
		h.processFormats(data[1:])
	case MSG_SNDIN_OPEN:
		h.progress("open")
		h.processOpen(data[1:])
	case MSG_SNDIN_FORMATCHANGE:
		h.progress("formatchange")
		h.processFormatChange(data[1:])
	default:
		slog.Debug("audin: unknown msgType", "type", fmt.Sprintf("0x%02x", data[0]))
	}
}

func (h *Handler) processVersion(body []byte) {
	if len(body) < 4 {
		return
	}
	serverVersion := binary.LittleEndian.Uint32(body)
	slog.Debug("audin: version", "server", serverVersion, "client", SNDIN_VERSION)
	if serverVersion > SNDIN_VERSION {
		slog.Warn("audin: incompatible server version", "server", serverVersion)
		return
	}
	out := make([]byte, 5)
	out[0] = MSG_SNDIN_VERSION
	binary.LittleEndian.PutUint32(out[1:], SNDIN_VERSION)
	_ = h.send(out)
}

func (h *Handler) processFormats(body []byte) {
	if len(body) < 8 {
		return
	}
	numFormats := binary.LittleEndian.Uint32(body)
	if numFormats < 1 || numFormats > 1000 {
		slog.Warn("audin: bad NumFormats", "n", numFormats)
		return
	}
	// body[4:8] is cbSizeFormatsPacket, ignored on receive.

	// Keep only PCM 16-bit formats we can capture from the browser.
	kept := make([]AudioFormat, 0, numFormats)
	off := 8
	for i := uint32(0); i < numFormats && off < len(body); i++ {
		var f AudioFormat
		f, off = unpackAudioFormat(body, off)
		if f.Tag == WAVE_FORMAT_PCM && f.BitsPerSample == 16 &&
			(f.Channels == 1 || f.Channels == 2) {
			kept = append(kept, f)
		}
	}

	h.mu.Lock()
	h.formats = kept
	h.mu.Unlock()
	slog.Debug("audin: formats", "server", numFormats, "kept", len(kept))

	if err := h.send([]byte{MSG_SNDIN_DATA_INCOM}); err != nil {
		return
	}

	size := 9
	for _, f := range kept {
		size += 18 + len(f.ExtraData)
	}
	out := make([]byte, 0, size)
	hdr := make([]byte, 9)
	hdr[0] = MSG_SNDIN_FORMATS
	binary.LittleEndian.PutUint32(hdr[1:], uint32(len(kept)))
	binary.LittleEndian.PutUint32(hdr[5:], uint32(size))
	out = append(out, hdr...)
	for _, f := range kept {
		out = append(out, f.pack()...)
	}
	_ = h.send(out)
}

func (h *Handler) processOpen(body []byte) {
	if len(body) < 8 {
		return
	}
	framesPerPacket := binary.LittleEndian.Uint32(body)
	initialFormat := binary.LittleEndian.Uint32(body[4:])

	// MS-RDPEAI 2.2.4: OPEN содержит полный WAVEFORMATEX сразу после
	// initialFormat, и именно он задаёт формат записи и размер Data PDU
	// (nChannels * 2 * FramesPerPacket). Индекс initialFormat — лишь
	// ссылка на список; если инлайновый формат расходится с нашим
	// списком, доверяем инлайновому (иначе размер пакета неверен и
	// Windows рвёт сессию).
	var srv AudioFormat
	if len(body) >= 8+18 {
		srv, _ = unpackAudioFormat(body, 8)
	}
	slog.Debug("audin: open", "framesPerPacket", framesPerPacket,
		"format", initialFormat,
		"inline", fmt.Sprintf("%dHz/%dch/%dbit", srv.SamplesPerSec, srv.Channels, srv.BitsPerSample))

	h.mu.Lock()
	valid := int(initialFormat) < len(h.formats)
	var f AudioFormat
	fromServer := srv.Channels > 0 && srv.SamplesPerSec > 0 && srv.BitsPerSample > 0
	if fromServer {
		f = srv
	} else if valid {
		f = h.formats[initialFormat]
	}
	h.mu.Unlock()

	if !fromServer && !valid {
		slog.Warn("audin: invalid format in OPEN", "idx", initialFormat)
		return
	}
	// Accept unconditionally (see SetEnabled): sending is still gated by
	// the UI toggle, so no audio leaks before the user arms the mic.

	bitsPerSample := int(f.BitsPerSample)
	if bitsPerSample <= 0 {
		bitsPerSample = 16
	}
	h.mu.Lock()
	if valid {
		h.current = int(initialFormat)
	}
	h.framesPerPacket = framesPerPacket
	h.bytesPerFrame = int(f.Channels) * (bitsPerSample / 8)
	h.pcmBuf = nil
	cb := h.onOpen
	h.mu.Unlock()

	if framesPerPacket > 0 {
		h.progress(fmt.Sprintf("fpp%d", framesPerPacket))
	}
	h.progress(fmt.Sprintf("srv%dx%dx%d", f.SamplesPerSec, f.Channels, bitsPerSample))
	if cb != nil {
		cb(MicFormat{
			SamplesPerSec:   f.SamplesPerSec,
			Channels:        f.Channels,
			FramesPerPacket: framesPerPacket,
		})
	}
	h.sendFormatChange(initialFormat)
	h.sendOpenReply(0)
}

func (h *Handler) processFormatChange(body []byte) {
	if len(body) < 4 {
		return
	}
	newFormat := binary.LittleEndian.Uint32(body)
	slog.Debug("audin: format change", "format", newFormat)

	h.mu.Lock()
	valid := int(newFormat) < len(h.formats)
	if valid {
		h.current = int(newFormat)
	}
	var f AudioFormat
	if valid {
		f = h.formats[h.current]
	}
	cb := h.onOpen
	h.mu.Unlock()

	if !valid {
		slog.Warn("audin: invalid format index", "idx", newFormat)
		return
	}
	if cb != nil {
		cb(MicFormat{SamplesPerSec: f.SamplesPerSec, Channels: f.Channels})
	}
	h.sendFormatChange(newFormat)
}

// CloseCapture resets the open state (channel teardown).
func (h *Handler) CloseCapture() {
	h.mu.Lock()
	h.current = -1
	cb := h.onClose
	h.mu.Unlock()
	if cb != nil {
		cb()
	}
}

func (h *Handler) sendFormatChange(idx uint32) {
	out := make([]byte, 5)
	out[0] = MSG_SNDIN_FORMATCHANGE
	binary.LittleEndian.PutUint32(out[1:], idx)
	_ = h.send(out)
}

func (h *Handler) sendOpenReply(result uint32) {
	out := make([]byte, 5)
	out[0] = MSG_SNDIN_OPEN_REPLY
	binary.LittleEndian.PutUint32(out[1:], result)
	_ = h.send(out)
}

func (h *Handler) send(data []byte) error {
	h.mu.Lock()
	viaDvc := h.viaDvc
	dvcSend := h.dvcSendFunc
	sender := h.channelSender
	h.mu.Unlock()
	if viaDvc && dvcSend != nil {
		dvcSend(data)
		return nil
	}
	if sender != nil {
		_, err := sender.SendToChannel(ChannelName, data)
		return err
	}
	return fmt.Errorf("audin: no channel sender")
}

// --- DVC adapter (mirrors rdpsnd.NewDvcAdapter) ---

// DvcAdapter routes a DVC "AUDIO_INPUT" channel into a Handler.
type DvcAdapter struct {
	handler  *Handler
	sendFunc func([]byte)
}

// NewDvcAdapter creates a DVC adapter for the given handler.
func NewDvcAdapter(handler *Handler) *DvcAdapter {
	return &DvcAdapter{handler: handler}
}

// Process implements drdynvc.DvcChannelHandler.
func (a *DvcAdapter) Process(data []byte) {
	a.handler.mu.Lock()
	first := !a.handler.sawDVC
	a.handler.sawDVC = true
	a.handler.viaDvc = true
	a.handler.dvcSendFunc = a.sendFunc
	a.handler.mu.Unlock()
	if first {
		a.handler.progress("dvc")
	}
	a.handler.ProcessData(data)
}

// SetSendFunc is called by the DVC client to provide the send function.
func (a *DvcAdapter) SetSendFunc(fn func([]byte)) {
	a.sendFunc = fn
}
