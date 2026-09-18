// Package rdpdr implements the minimal client side of the File System
// Virtual Channel Extension (MS-RDPEFS) handshake.
//
// The handshake is not needed to redirect anything by itself, but Windows
// withholds other redirected resources (notably audio input redirection,
// AUDIO_INPUT) until the device-redirection channel is initialized. grdp used
// to register "rdpdr" as a silent stub, so the server kept waiting for the
// Client Announce Reply and never offered the microphone channel.
//
// Only the mandatory core exchange is implemented; no devices are announced:
//
//	S→C Server Announce Request   | C→S Client Announce Reply + Client Name Request
//	S→C Server Client ID Confirm   | (ignored)
//	S→C Server Core Capability Req | C→S Client Core Capability Response (GENERAL only)
//
// Reference: FreeRDP channels/rdpdr/client/rdpdr_main.c, rdpdr_capabilities.c
// (MS-RDPEFS 2.2, 3.1.4).
package rdpdr

import (
	"encoding/binary"
	"fmt"
	"log/slog"
	"sync"
	"unicode/utf16"

	"github.com/nakagami/grdp/core"
	"github.com/nakagami/grdp/plugin"
)

const (
	ChannelName   = "rdpdr"
	ChannelOption = plugin.CHANNEL_OPTION_INITIALIZED |
		plugin.CHANNEL_OPTION_ENCRYPT_RDP |
		plugin.CHANNEL_OPTION_COMPRESS_RDP
)

// Component and packet identifiers (MS-RDPEFS 2.2.1, 2.2.2; see also
// FreeRDP include/freerdp/channels/rdpdr.h).
const (
	ctypCore                    = 0x4472 // "Dr"
	pakidCoreServerAnnounce     = 0x496E // "In"
	pakidCoreClientIDConfirm    = 0x4343 // "CC"
	pakidCoreClientName         = 0x434E // "CN"
	pakidCoreDeviceListAnnounce = 0x4441 // "DA"
	pakidCoreServerCapability   = 0x5350 // "SP"
	pakidCoreClientCapability   = 0x4350 // "CP"
	pakidCoreUserLoggedOn       = 0x554C // "UL"
)

// Protocol versions (MS-RDPEFS 1.7). The client reports the lower of its own
// version and the server's.
const (
	versionMajor    = 1
	versionMinorMax = 13
)

// GENERAL capability set flags (MS-RDPEFS 2.2.5.1). Mirrors FreeRDP defaults.
const (
	rdpdrDeviceRemovePDUs     = 0x00000001
	rdpdrClientDisplayNamePDU = 0x00000002
	rdpdrUserLoggedOnPDU      = 0x00000004
	enableAsyncIO             = 0x00000001
	// All IRP major codes: with no redirected devices nothing is invoked,
	// but servers expect a plausible mask (FreeRDP advertises the full set).
	irpCodeMask = 0x0000FFFF
)

// Capability set identifiers (MS-RDPEFS 2.2.5).
const (
	capGeneralType       = 0x0001
	generalCapVersion02  = 0x00000002
	capabilityHeaderSize = 8
	generalCapBodySize   = 36
)

// Handler implements the mandatory rdpdr core exchange over the static
// "rdpdr" virtual channel. It announces no devices.
type Handler struct {
	mu       sync.Mutex
	sender   core.ChannelSender
	clientID uint32
	major    uint16
	minor    uint16

	// onStage reports handshake progress for diagnostics.
	onStage func(string)
}

// NewHandler creates a new rdpdr core-exchange handler.
func NewHandler() *Handler {
	return &Handler{major: versionMajor, minor: versionMinorMax}
}

// SetStageCallback installs a diagnostics callback reporting handshake steps.
func (h *Handler) SetStageCallback(f func(string)) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.onStage = f
}

func (h *Handler) stage(s string) {
	h.mu.Lock()
	f := h.onStage
	h.mu.Unlock()
	if f != nil {
		f(s)
	}
}

// --- plugin.ChannelTransport interface ---

func (h *Handler) GetType() (string, uint32) {
	return ChannelName, ChannelOption
}

func (h *Handler) Sender(s core.ChannelSender) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.sender = s
}

// Process handles one reassembled rdpdr PDU.
func (h *Handler) Process(data []byte) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("rdpdr: panic in Process", "err", r)
		}
	}()
	if len(data) < 4 {
		return
	}
	component := binary.LittleEndian.Uint16(data[0:])
	packetID := binary.LittleEndian.Uint16(data[2:])
	if component != ctypCore {
		return
	}
	body := data[4:]
	switch packetID {
	case pakidCoreServerAnnounce:
		h.processServerAnnounce(body)
	case pakidCoreServerCapability:
		h.processServerCapability(body)
	case pakidCoreClientIDConfirm:
		// Server confirms the client id we sent; nothing to answer.
	default:
		slog.Debug("rdpdr: unhandled packet", "id", packetID)
		h.stage(fmt.Sprintf("pdr%04x", packetID))
	}
}

// processServerAnnounce answers the Server Announce Request with the Client
// Announce Reply (immediately followed by the Client Name Request, as required
// by MS-RDPEFS 3.1.4).
func (h *Handler) processServerAnnounce(body []byte) {
	if len(body) < 8 {
		return
	}
	serverMajor := binary.LittleEndian.Uint16(body[0:])
	serverMinor := binary.LittleEndian.Uint16(body[2:])
	clientID := binary.LittleEndian.Uint32(body[4:])

	h.mu.Lock()
	h.clientID = clientID
	h.major = min(versionMajor, serverMajor)
	h.minor = min(versionMinorMax, serverMinor)
	major, minor := h.major, h.minor
	h.mu.Unlock()

	// Client Announce Reply: component, packet id, versions, client id.
	out := make([]byte, 12)
	binary.LittleEndian.PutUint16(out[0:], ctypCore)
	binary.LittleEndian.PutUint16(out[2:], pakidCoreClientIDConfirm)
	binary.LittleEndian.PutUint16(out[4:], major)
	binary.LittleEndian.PutUint16(out[6:], minor)
	binary.LittleEndian.PutUint32(out[8:], clientID)
	h.send(out)
	h.stage("rdpdr-ann")

	// Client Name Request: unicodeFlag=1, codePage=0, name (UTF-16LE, NUL).
	name := utf16.Encode([]rune("EntwareManager"))
	nameBytes := make([]byte, (len(name)+1)*2)
	for i, u := range name {
		binary.LittleEndian.PutUint16(nameBytes[i*2:], u)
	}
	req := make([]byte, 16+len(nameBytes))
	binary.LittleEndian.PutUint16(req[0:], ctypCore)
	binary.LittleEndian.PutUint16(req[2:], pakidCoreClientName)
	binary.LittleEndian.PutUint32(req[4:], 1) // unicodeFlag
	binary.LittleEndian.PutUint32(req[8:], 0) // codePage
	binary.LittleEndian.PutUint32(req[12:], uint32(len(nameBytes)))
	copy(req[16:], nameBytes)
	h.send(req)
	h.stage("rdpdr-name")
}

// processServerCapability answers the Server Core Capability Request with a
// Client Core Capability Response carrying a single GENERAL capability set.
// The server's ioCode1 is echoed back (masked) like FreeRDP does.
func (h *Handler) processServerCapability(body []byte) {
	serverIOCode1 := h.parseServerIOCode1(body)

	h.mu.Lock()
	major, minor := h.major, h.minor
	h.mu.Unlock()

	const capLen = capabilityHeaderSize + generalCapBodySize
	out := make([]byte, 8+capLen)
	binary.LittleEndian.PutUint16(out[0:], ctypCore)
	binary.LittleEndian.PutUint16(out[2:], pakidCoreClientCapability)
	binary.LittleEndian.PutUint16(out[4:], 1) // numCapabilities
	binary.LittleEndian.PutUint16(out[6:], 0) // pad

	// RDPDR_CAPABILITY_HEADER
	binary.LittleEndian.PutUint16(out[8:], capGeneralType)
	binary.LittleEndian.PutUint16(out[10:], capLen)
	binary.LittleEndian.PutUint32(out[12:], generalCapVersion02)

	// GENERAL_CAPABILITYSET body. No devices are redirected, but the
	// protocol/IRP fields mirror FreeRDP so the server sees a normal peer.
	o := out[16:]
	binary.LittleEndian.PutUint32(o[0:], 0)                                                                     // osType, ignored on receipt
	binary.LittleEndian.PutUint32(o[4:], 0)                                                                     // osVersion, must be zero
	binary.LittleEndian.PutUint16(o[8:], major)                                                                 // protocolMajorVersion
	binary.LittleEndian.PutUint16(o[10:], minor)                                                                // protocolMinorVersion
	binary.LittleEndian.PutUint32(o[12:], irpCodeMask&serverIOCode1)                                            // ioCode1
	binary.LittleEndian.PutUint32(o[16:], 0)                                                                    // ioCode2, must be zero
	binary.LittleEndian.PutUint32(o[20:], rdpdrDeviceRemovePDUs|rdpdrClientDisplayNamePDU|rdpdrUserLoggedOnPDU) // extendedPDU
	binary.LittleEndian.PutUint32(o[24:], enableAsyncIO)                                                        // extraFlags1
	binary.LittleEndian.PutUint32(o[28:], 0)                                                                    // extraFlags2, must be zero
	binary.LittleEndian.PutUint32(o[32:], 0)                                                                    // SpecialTypeDeviceCap

	h.send(out)
	h.stage("rdpdr-caps")
}

// parseServerIOCode1 extracts ioCode1 from the server's GENERAL capability
// set inside a Server Core Capability Request.
func (h *Handler) parseServerIOCode1(body []byte) uint32 {
	if len(body) < 4 {
		return 0
	}
	count := int(binary.LittleEndian.Uint16(body))
	off := 4
	for i := 0; i < count && off+capabilityHeaderSize <= len(body); i++ {
		capType := binary.LittleEndian.Uint16(body[off:])
		capLen := int(binary.LittleEndian.Uint16(body[off+2:]))
		if capLen < capabilityHeaderSize || off+capLen > len(body) {
			break
		}
		if capType == capGeneralType && capLen >= capabilityHeaderSize+20 {
			// body: osType(4) osVersion(4) major(2) minor(2) ioCode1(4) ...
			return binary.LittleEndian.Uint32(body[off+capabilityHeaderSize+12:])
		}
		off += capLen
	}
	return 0
}

func (h *Handler) send(data []byte) {
	h.mu.Lock()
	s := h.sender
	h.mu.Unlock()
	if s == nil {
		return
	}
	if _, err := s.SendToChannel(ChannelName, data); err != nil {
		slog.Debug("rdpdr: send failed", "err", err)
	}
}
