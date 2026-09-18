package drdynvc

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"

	"github.com/nakagami/grdp/core"
	"github.com/nakagami/grdp/plugin"
)

const (
	ChannelName   = plugin.DRDYNVC_SVC_CHANNEL_NAME
	ChannelOption = plugin.CHANNEL_OPTION_INITIALIZED |
		plugin.CHANNEL_OPTION_ENCRYPT_RDP
)

const (
	MAX_DVC_CHANNELS = 20
)

const (
	DYNVC_CREATE_REQ            = 0x01
	DYNVC_DATA_FIRST            = 0x02
	DYNVC_DATA                  = 0x03
	DYNVC_CLOSE                 = 0x04
	DYNVC_CAPABILITIES          = 0x05
	DYNVC_DATA_FIRST_COMPRESSED = 0x06
	DYNVC_DATA_COMPRESSED       = 0x07
	DYNVC_SOFT_SYNC_REQUEST     = 0x08
	DYNVC_SOFT_SYNC_RESPONSE    = 0x09
)

// DvcChannelHandler processes data for a specific dynamic virtual channel.
type DvcChannelHandler interface {
	Process(data []byte)
}

type ChannelClient struct {
	name          string
	id            uint32
	channelSender core.ChannelSender
}

type dvcChannelInfo struct {
	name    string
	id      uint32
	cbChId  uint8
	handler DvcChannelHandler
}

type dvcReassembly struct {
	buf      bytes.Buffer
	totalLen uint32
}

type DvcClient struct {
	w                 core.ChannelSender
	channels          map[string]ChannelClient
	handlers          map[string]DvcChannelHandler // channelName → handler
	rejectedChannels  map[string]bool              // channelName → explicitly rejected
	channelById       map[uint32]*dvcChannelInfo   // channelId → info
	reassembly        map[uint32]*dvcReassembly    // channelId → reassembly state
	negotiatedVersion uint16

	// pendingCreate maps a channel name we asked the server to create to
	// the id we chose, so the server's create response can be matched.
	pendingCreate map[string]uint32

	// onCreateReq is a diagnostics observer for incoming create requests.
	onCreateReq func(string)

	// onReady fires once when the DVC layer is usable (capabilities
	// negotiated), so the client may create its own channels.
	onReady  func()
	readyOne sync.Once

	// confirmedChannels tracks client-initiated channels the server accepted.
	confirmedChannels map[string]bool

	// onCreateRsp reports the status for a channel WE asked to create.
	onCreateRsp func(name string, status uint32)

	// onUnknownCmd reports a DVC PDU command we do not handle (diagnostics).
	onUnknownCmd func(cmd uint8)

	// capsSent guards the one-time DVC capabilities response.
	capsSent bool
}

func NewDvcClient() *DvcClient {
	return &DvcClient{
		channels:          make(map[string]ChannelClient, 100),
		handlers:          make(map[string]DvcChannelHandler),
		rejectedChannels:  make(map[string]bool),
		channelById:       make(map[uint32]*dvcChannelInfo),
		reassembly:        make(map[uint32]*dvcReassembly),
		pendingCreate:     make(map[string]uint32),
		confirmedChannels: make(map[string]bool),
	}
}

// nextChannelId picks a channel id not already in use. Client-initiated
// channels must not collide with ids the server assigned.
func (c *DvcClient) nextChannelId() (uint32, bool) {
	for id := uint32(1); id < 0xFFFF; id++ {
		if _, ok := c.channelById[id]; ok {
			continue
		}
		used := false
		for _, pend := range c.pendingCreate {
			if pend == id {
				used = true
				break
			}
		}
		if !used {
			return id, true
		}
	}
	return 0, false
}

// OpenChannel sends a client-initiated DYNVC_CREATE_REQ for the named
// channel (used for AUDIO_INPUT: the microphone channel is created by the
// client, like mstsc does when "Record from this computer" is on).
// The server answers with a create response carrying a status.
func (c *DvcClient) OpenChannel(name string) error {
	if c.w == nil {
		return fmt.Errorf("dvc: channel sender not available")
	}
	if _, ok := c.channels[name]; ok {
		return nil
	}
	id, ok := c.nextChannelId()
	if !ok {
		return fmt.Errorf("dvc: no free channel id")
	}
	hdr := &DvcHeader{cmd: DYNVC_CREATE_REQ, sp: 0, cbChId: 0}
	b := &bytes.Buffer{}
	b.Write(hdr.serialize(id))
	b.WriteString(name)
	b.WriteByte(0)

	c.channels[name] = ChannelClient{name: name, id: id}
	c.pendingCreate[name] = id
	if _, err := c.Send(b.Bytes()); err != nil {
		delete(c.channels, name)
		delete(c.pendingCreate, name)
		return err
	}
	slog.Debug("dvc: create request sent", "channel", name, "id", id)
	return nil
}

// RetryChannelRequest (re)sends a create request for a client-initiated
// channel. Unlike OpenChannel it ignores an existing pending entry, so the
// caller can retry when the server did not answer the first attempt.
func (c *DvcClient) RetryChannelRequest(name string) error {
	if c.w == nil {
		return fmt.Errorf("dvc: channel sender not available")
	}
	if c.confirmedChannels[name] {
		return nil
	}
	// Drop a stale pending id for this name before reusing the name.
	delete(c.pendingCreate, name)
	id, ok := c.nextChannelId()
	if !ok {
		return fmt.Errorf("dvc: no free channel id")
	}
	hdr := &DvcHeader{cmd: DYNVC_CREATE_REQ, sp: 0, cbChId: 0}
	b := &bytes.Buffer{}
	b.Write(hdr.serialize(id))
	b.WriteString(name)
	b.WriteByte(0)
	c.channels[name] = ChannelClient{name: name, id: id}
	c.pendingCreate[name] = id
	if _, err := c.Send(b.Bytes()); err != nil {
		delete(c.channels, name)
		delete(c.pendingCreate, name)
		return err
	}
	slog.Debug("dvc: create request retried", "channel", name, "id", id)
	return nil
}

// ConfirmChannel records a successful client-initiated channel creation.
func (c *DvcClient) ConfirmChannel(name string) {
	c.confirmedChannels[name] = true
}

// RegisterHandler registers a handler for a named DVC channel.
func (c *DvcClient) RegisterHandler(name string, handler DvcChannelHandler) {
	c.handlers[name] = handler
}

// RegisterRejectedChannel marks a DVC channel to be explicitly rejected
// (non-zero CreationStatus) so the server does not use it.
// Use this to steer servers toward a fallback channel; for example,
// rejecting AUDIO_PLAYBACK_LOSSY_DVC forces gnome-remote-desktop to
// fall back to lossless AUDIO_PLAYBACK_DVC (PCM).
func (c *DvcClient) RegisterRejectedChannel(name string) {
	c.rejectedChannels[name] = true
}

// SetCreateReqObserver installs a diagnostics callback invoked with the
// name of every channel the server asks to create.
func (c *DvcClient) SetCreateReqObserver(f func(string)) {
	c.onCreateReq = f
}

// SetReadyCallback installs a callback fired once when the DVC layer is
// ready for client-initiated channel creation.
func (c *DvcClient) SetReadyCallback(f func()) {
	c.onReady = f
}

// NegotiatedVersion returns the DVC protocol version agreed with the server.
func (c *DvcClient) NegotiatedVersion() uint16 {
	return c.negotiatedVersion
}

// SetCreateRspCallback installs a callback reporting the server's status
// for a client-initiated channel creation.
func (c *DvcClient) SetCreateRspCallback(f func(string, uint32)) {
	c.onCreateRsp = f
}

// SetUnknownCmdCallback installs a diagnostics callback for DVC PDUs with
// a command we do not recognize.
func (c *DvcClient) SetUnknownCmdCallback(f func(uint8)) {
	c.onUnknownCmd = f
}

// markReady fires the ready callback at most once.
func (c *DvcClient) markReady() {
	c.readyOne.Do(func() {
		if c.onReady != nil {
			c.onReady()
		}
	})
}

func (c *DvcClient) LoadAddin(f core.ChannelSender) {

}

type DvcHeader struct {
	cmd    uint8
	sp     uint8
	cbChId uint8
}

func readHeader(r io.Reader) *DvcHeader {
	value, _ := core.ReadUInt8(r)
	cmd := (value & 0xf0) >> 4
	sp := (value & 0x0c) >> 2
	cbChId := (value & 0x03) >> 0
	return &DvcHeader{cmd, sp, cbChId}
}

func (h *DvcHeader) serialize(channelId uint32) []byte {
	b := &bytes.Buffer{}
	core.WriteUInt8((h.cmd<<4)|(h.sp<<2)|h.cbChId, b)
	if h.cbChId == 0 {
		core.WriteUInt8(uint8(channelId), b)
	} else if h.cbChId == 1 {
		core.WriteUInt16LE(uint16(channelId), b)
	} else {
		core.WriteUInt32LE(channelId, b)
	}

	return b.Bytes()
}

func (c *DvcClient) Send(s []byte) (int, error) {
	slog.Debug("dvc Send", "len", len(s), "data", hex.EncodeToString(s))
	name, _ := c.GetType()
	return c.w.SendToChannel(name, s)
}

// SendDvcData sends data on a DVC channel wrapped in a DYNVC_DATA PDU.
// dvcIdLen — сколько байт занимает ChannelId при данном cbChId.
func dvcIdLen(cbChId uint8) int {
	switch cbChId {
	case 1:
		return 2
	case 2, 3:
		return 4
	default:
		return 1
	}
}

// sendDataPdu отправляет одиночный DYNVC_DATA (0x03): заголовок + порция.
func (c *DvcClient) sendDataPdu(ch *dvcChannelInfo, channelId uint32, data []byte) {
	hdr := &DvcHeader{cmd: DYNVC_DATA, sp: 0, cbChId: ch.cbChId}
	b := &bytes.Buffer{}
	b.Write(hdr.serialize(channelId))
	b.Write(data)
	c.Send(b.Bytes())
}

// SendDvcData отправляет данные по DVC-каналу. Если порция не помещается
// в CHANNEL_CHUNK_LENGTH, она режется на Data First PDU (0x02, с полной
// длиной; размер поля длины кодируется в Sp) + Data PDU (0x03) — как в
// MS-RDPEDYC 2.2.4 и FreeRDP drdynvc_write_data. Отдавать такую порцию
// транспортному чанкеру нельзя: Windows не собирает DVC из нескольких
// VC-чанков и рвёт соединение (проверено на аудиовходе, 1765 байт).
func (c *DvcClient) SendDvcData(channelId uint32, data []byte) {
	ch, ok := c.channelById[channelId]
	if !ok {
		return
	}
	const maxChunk = plugin.CHANNEL_CHUNK_LENGTH
	idLen := dvcIdLen(ch.cbChId)

	if len(data)+1+idLen <= maxChunk {
		c.sendDataPdu(ch, channelId, data)
		return
	}

	// Размер поля длины: 1 байт (<=0xFF), 2 байта (<=0xFFFF), иначе 4.
	cb := 0
	if len(data) > 0xFF {
		cb = 1
	}
	if len(data) > 0xFFFF {
		cb = 2
	}
	lenBytes := 1 << cb

	b := &bytes.Buffer{}
	hdr := &DvcHeader{cmd: DYNVC_DATA_FIRST, sp: uint8(cb), cbChId: ch.cbChId}
	b.Write(hdr.serialize(channelId))
	switch cb {
	case 0:
		b.WriteByte(byte(len(data)))
	case 1:
		var l [2]byte
		binary.LittleEndian.PutUint16(l[:], uint16(len(data)))
		b.Write(l[:])
	default:
		var l [4]byte
		binary.LittleEndian.PutUint32(l[:], uint32(len(data)))
		b.Write(l[:])
	}
	firstMax := maxChunk - (1 + idLen + lenBytes)
	if firstMax <= 0 {
		firstMax = 1
	}
	first := data
	if len(first) > firstMax {
		first = data[:firstMax]
	}
	b.Write(first)
	c.Send(b.Bytes())

	rest := data[len(first):]
	contMax := maxChunk - (1 + idLen)
	for len(rest) > 0 {
		n := len(rest)
		if n > contMax {
			n = contMax
		}
		c.sendDataPdu(ch, channelId, rest[:n])
		rest = rest[n:]
	}
}
func (c *DvcClient) Sender(f core.ChannelSender) {
	c.w = f
}
func (c *DvcClient) GetType() (string, uint32) {
	return ChannelName, ChannelOption
}

func (c *DvcClient) Process(s []byte) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("dvc: panic in Process", "err", r)
		}
	}()
	r := bytes.NewReader(s)
	hdr := readHeader(r)
	b, _ := core.ReadBytes(r.Len(), r)

	// Robustness: a create response for a client-initiated channel may be
	// encoded with the CREATE_REQ cmd (0x01) or, per some encoders, with a
	// distinct value. If the payload matches a channel we asked to create
	// (id + 4-byte status only), handle it as the response.
	if hdr.cmd != DYNVC_CREATE_REQ && c.isPendingCreateResponse(hdr, b) {
		slog.Debug("dvc: create response (alt cmd)", "cmd", hdr.cmd)
		c.processCreateReq(hdr, b)
		return
	}

	switch hdr.cmd {
	case DYNVC_CAPABILITIES:
		slog.Debug("DYNVC_CAPABILITIES")
		c.processCapsPdu(hdr, b)
	case DYNVC_CREATE_REQ:
		slog.Debug("DYNVC_CREATE_REQ")
		c.processCreateReq(hdr, b)
	case DYNVC_DATA_FIRST:
		c.processDataFirst(hdr, b)
	case DYNVC_DATA:
		c.processData(hdr, b)
	case DYNVC_CLOSE:
		c.processClose(hdr, b)
	case DYNVC_SOFT_SYNC_REQUEST:
		slog.Debug("DYNVC_SOFT_SYNC_REQUEST")
		c.processSoftSyncRequest(hdr, b)
	default:
		slog.Warn("dvc: unhandled cmd", "cmd", hdr.cmd)
		if c.onUnknownCmd != nil {
			c.onUnknownCmd(hdr.cmd)
		}
	}
}
func (c *DvcClient) processClose(hdr *DvcHeader, s []byte) {
	r := bytes.NewReader(s)
	channelId := readDvcId(r, hdr.cbChId)
	ch, ok := c.channelById[channelId]
	name := "(unknown)"
	if ok {
		name = ch.name
		delete(c.channelById, channelId)
		delete(c.reassembly, channelId)
	}
	slog.Debug("dvc: CLOSE", "channelId", channelId, "name", name)
}

func (c *DvcClient) bindChannel(channelId uint32, channelName string, cbChId uint8) DvcChannelHandler {
	h, ok := c.handlers[channelName]
	if !ok {
		return nil
	}
	c.channelById[channelId] = &dvcChannelInfo{
		name:    channelName,
		id:      channelId,
		cbChId:  cbChId,
		handler: h,
	}
	// Provide send callback if handler supports it
	if setter, ok := h.(interface{ SetSendFunc(func([]byte)) }); ok {
		chId := channelId
		setter.SetSendFunc(func(data []byte) {
			c.SendDvcData(chId, data)
		})
	}
	slog.Debug("dvc: handler registered", "channel", channelName, "id", channelId)
	return h
}

// ensureCaps sends the DVC capabilities response at most once. It must
// precede any client-initiated create request: Windows silently drops
// create requests that arrive before the capabilities exchange
// (FreeRDP works around the same behavior when the server skips CAPS).
func (c *DvcClient) ensureCaps(ver uint16) {
	if c.capsSent {
		return
	}
	c.capsSent = true
	if ver == 0 || ver > 3 {
		ver = 3
	}
	b := &bytes.Buffer{}
	core.WriteUInt8(0x5c, b) // header: Cmd=5(CAPS), Sp=3, CbChId=0 (mstsc-like)
	core.WriteUInt8(0x00, b) // pad
	core.WriteUInt16LE(ver, b)
	slog.Debug("dvc: CAPS response", "version", ver)
	c.Send(b.Bytes())
	c.negotiatedVersion = ver
}

func (c *DvcClient) processCreateReq(hdr *DvcHeader, s []byte) {
	// A create request implies the DVC layer is live even if the server
	// never sent its CAPS PDU; make sure our CAPS response went first.
	c.ensureCaps(c.negotiatedVersion)

	r := bytes.NewReader(s)
	channelId := readDvcId(r, hdr.cbChId)

	// Response to a channel WE asked the server to create: it carries only
	// a 4-byte CreationStatus (no channel name). Match by pending id.
	if name, ok := c.pendingNameById(channelId); ok && r.Len() == 4 {
		status, _ := core.ReadUInt32LE(r)
		delete(c.pendingCreate, name)
		slog.Debug("dvc: create response", "channel", name, "id", channelId, "status", status)
		if c.onCreateRsp != nil {
			c.onCreateRsp(name, status)
		}
		if status != 0 {
			slog.Warn("dvc: channel creation failed", "channel", name, "status", status)
			delete(c.channels, name)
			return
		}
		c.ConfirmChannel(name)
		h := c.bindChannel(channelId, name, hdr.cbChId)
		if h != nil {
			if ch, ok := h.(interface{ OnChannelCreated() }); ok {
				ch.OnChannelCreated()
			}
		}
		return
	}

	nameBytes, _ := core.ReadBytes(r.Len(), r)
	channelName := strings.TrimRight(string(nameBytes), "\x00")
	slog.Debug("dvc: create request", "channelId", channelId, "name", channelName)

	// Diagnostics: report every requested channel name.
	if c.onCreateReq != nil {
		c.onCreateReq(channelName)
	}

	// Associate handler if registered
	c.bindChannel(channelId, channelName, hdr.cbChId)

	// If explicitly rejected, send a non-zero CreationStatus so the server
	// does not use this channel (e.g. AUDIO_PLAYBACK_LOSSY_DVC → fallback to PCM).
	if c.rejectedChannels[channelName] {
		slog.Debug("dvc: rejecting channel", "channel", channelName, "id", channelId)
		rspHdr := &DvcHeader{cmd: DYNVC_CREATE_REQ, sp: 0, cbChId: hdr.cbChId}
		b := &bytes.Buffer{}
		b.Write(rspHdr.serialize(channelId))
		core.WriteUInt32LE(0x80004005, b) // E_FAIL
		c.Send(b.Bytes())
		return
	}

	// Send success response (Sp SHOULD be 0 per MS-RDPEDYC 2.2.2.2).
	// Always accept: some Windows servers stop sending data on static
	// virtual channels (e.g. cliprdr) when DVC creation requests are
	// rejected, even for unrelated channels.
	rspHdr := &DvcHeader{cmd: DYNVC_CREATE_REQ, sp: 0, cbChId: hdr.cbChId}
	b := &bytes.Buffer{}
	b.Write(rspHdr.serialize(channelId))
	core.WriteUInt32LE(0, b)
	c.Send(b.Bytes())

	// Notify handler that channel is ready (CREATE_RSP has been sent)
	if h := c.bindChannel(channelId, channelName, hdr.cbChId); h != nil {
		if ch, ok := h.(interface{ OnChannelCreated() }); ok {
			ch.OnChannelCreated()
		}
	}
	// A server-initiated create implies the DVC layer is usable even if
	// the CAPS PDU never arrived (some servers skip it).
	c.markReady()
}

// pendingNameById returns the channel name for an id we asked the server
// to create, if any.
func (c *DvcClient) pendingNameById(id uint32) (string, bool) {
	for name, pend := range c.pendingCreate {
		if pend == id {
			return name, true
		}
	}
	return "", false
}

// isPendingCreateResponse reports whether the PDU payload looks like the
// status-only response to a channel creation we requested.
func (c *DvcClient) isPendingCreateResponse(hdr *DvcHeader, b []byte) bool {
	if len(c.pendingCreate) == 0 {
		return false
	}
	r := bytes.NewReader(b)
	id := readDvcId(r, hdr.cbChId)
	if _, ok := c.pendingNameById(id); !ok {
		return false
	}
	return r.Len() == 4
}

func readDvcId(r io.Reader, cbLen uint8) (id uint32) {
	switch cbLen {
	case 0:
		i, _ := core.ReadUInt8(r)
		id = uint32(i)
	case 1:
		i, _ := core.ReadUint16LE(r)
		id = uint32(i)
	default:
		id, _ = core.ReadUInt32LE(r)
	}
	return
}
func (c *DvcClient) processDataFirst(hdr *DvcHeader, s []byte) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("dvc: panic in processDataFirst", "err", r)
		}
	}()
	r := bytes.NewReader(s)
	channelId := readDvcId(r, hdr.cbChId)

	// Read total length (encoding based on sp/Len field)
	var totalLen uint32
	switch hdr.sp {
	case 0:
		l, _ := core.ReadUInt8(r)
		totalLen = uint32(l)
	case 1:
		l, _ := core.ReadUint16LE(r)
		totalLen = uint32(l)
	default:
		totalLen, _ = core.ReadUInt32LE(r)
	}

	data, _ := core.ReadBytes(r.Len(), r)
	ch, ok := c.channelById[channelId]
	if !ok || ch.handler == nil {
		return
	}

	if uint32(len(data)) >= totalLen {
		ch.handler.Process(data[:totalLen])
	} else {
		ra := &dvcReassembly{totalLen: totalLen}
		ra.buf.Write(data)
		c.reassembly[channelId] = ra
	}
}

func (c *DvcClient) processData(hdr *DvcHeader, s []byte) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("dvc: panic in processData", "err", r)
		}
	}()
	r := bytes.NewReader(s)
	channelId := readDvcId(r, hdr.cbChId)
	data, _ := core.ReadBytes(r.Len(), r)

	ch, ok := c.channelById[channelId]
	if !ok || ch.handler == nil {
		return
	}

	ra, hasReassembly := c.reassembly[channelId]
	if hasReassembly {
		ra.buf.Write(data)
		if uint32(ra.buf.Len()) >= ra.totalLen {
			ch.handler.Process(ra.buf.Bytes()[:ra.totalLen])
			delete(c.reassembly, channelId)
		}
	} else {
		ch.handler.Process(data)
	}
}

func (c *DvcClient) processCapsPdu(hdr *DvcHeader, s []byte) {
	r := bytes.NewReader(s)
	core.ReadUInt8(r)
	ver, _ := core.ReadUint16LE(r)
	slog.Debug("Server supports dvc", "version", ver)

	// Respond with the server's version (up to 3).
	// Version 3 is required for some servers to activate RDPGFX.
	ver = min(ver, 3)
	c.ensureCaps(ver)
	c.markReady()
}

func (c *DvcClient) processSoftSyncRequest(hdr *DvcHeader, s []byte) {
	r := bytes.NewReader(s)
	core.ReadUInt8(r)                 // Pad
	length, _ := core.ReadUInt32LE(r) // Length
	flags, _ := core.ReadUint16LE(r)  // Flags
	numTunnels, _ := core.ReadUint16LE(r)
	slog.Debug("DYNVC_SOFT_SYNC_REQUEST", "length", length, "flags", flags, "numTunnels", numTunnels)

	// Send SOFT_SYNC_RESPONSE: header + pad + length(4)
	b := &bytes.Buffer{}
	core.WriteUInt8((DYNVC_SOFT_SYNC_RESPONSE<<4)|0x00, b) // cmd=9, sp=0, cbChId=0
	core.WriteUInt8(0, b)                                  // Pad
	core.WriteUInt32LE(0x04, b)                            // Length = 4 (just the length field)
	c.Send(b.Bytes())
}
