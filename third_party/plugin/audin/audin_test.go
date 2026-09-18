package audin

import (
	"bytes"
	"encoding/binary"
	"testing"
)

type fakeSender struct {
	sent [][]byte
}

func (f *fakeSender) SendToChannel(channel string, s []byte) (int, error) {
	cp := make([]byte, len(s))
	copy(cp, s)
	f.sent = append(f.sent, cp)
	return len(s), nil
}

func waveFormatEx(tag uint16, ch uint16, rate uint32, bits uint16) []byte {
	b := make([]byte, 18)
	binary.LittleEndian.PutUint16(b[0:], tag)
	binary.LittleEndian.PutUint16(b[2:], ch)
	binary.LittleEndian.PutUint32(b[4:], rate)
	blk := int(ch) * int(bits) / 8
	binary.LittleEndian.PutUint32(b[8:], uint32(int(rate)*blk))
	binary.LittleEndian.PutUint16(b[12:], uint16(blk))
	binary.LittleEndian.PutUint16(b[14:], bits)
	binary.LittleEndian.PutUint16(b[16:], 0)
	return b
}

func TestHandshake(t *testing.T) {
	h := NewHandler()
	s := &fakeSender{}
	h.Sender(s)
	h.SetEnabled(true)

	var opened MicFormat
	gotOpen := false
	h.SetOpenCallback(func(f MicFormat) { opened = f; gotOpen = true })

	// 1. VERSION: server 2 → client replies version 2.
	h.Process([]byte{MSG_SNDIN_VERSION, 2, 0, 0, 0})
	if len(s.sent) != 1 || !bytes.Equal(s.sent[0], []byte{MSG_SNDIN_VERSION, 2, 0, 0, 0}) {
		t.Fatalf("bad version reply: %v", s.sent)
	}

	// 2. FORMATS: PCM16 stereo 44100 + MP3 (unsupported) → keep 1.
	body := []byte{MSG_SNDIN_FORMATS}
	hdr := make([]byte, 8)
	binary.LittleEndian.PutUint32(hdr[0:], 2)
	binary.LittleEndian.PutUint32(hdr[4:], 8+18+18)
	body = append(body, hdr...)
	body = append(body, waveFormatEx(1, 2, 44100, 16)...)
	body = append(body, waveFormatEx(0x55, 2, 44100, 16)...)
	h.Process(body)
	if len(s.sent) != 3 {
		t.Fatalf("expected incoming+formats, got %d sends", len(s.sent))
	}
	if !bytes.Equal(s.sent[1], []byte{MSG_SNDIN_DATA_INCOM}) {
		t.Fatalf("expected DATA_INCOMING first, got %v", s.sent[1])
	}
	reply := s.sent[2]
	if reply[0] != MSG_SNDIN_FORMATS || binary.LittleEndian.Uint32(reply[1:]) != 1 {
		t.Fatalf("bad formats reply: %v", reply)
	}

	// 3. OPEN format 0 → FORMATCHANGE + OPEN_REPLY(0), onOpen fired.
	open := []byte{MSG_SNDIN_OPEN, 1, 0, 0, 0, 0, 0, 0, 0}
	h.Process(open)
	if len(s.sent) != 5 {
		t.Fatalf("expected formatchange+openreply, got %d sends", len(s.sent))
	}
	if !bytes.Equal(s.sent[3], []byte{MSG_SNDIN_FORMATCHANGE, 0, 0, 0, 0}) {
		t.Fatalf("bad formatchange: %v", s.sent[3])
	}
	if !bytes.Equal(s.sent[4], []byte{MSG_SNDIN_OPEN_REPLY, 0, 0, 0, 0}) {
		t.Fatalf("bad open reply: %v", s.sent[4])
	}
	if !gotOpen || opened.SamplesPerSec != 44100 || opened.Channels != 2 {
		t.Fatalf("onOpen not fired correctly: %+v", opened)
	}

	// 4. PushPCM → DATA_INCOMING + DATA.
	h.PushPCM([]byte{0x11, 0x22, 0x33, 0x44})
	if len(s.sent) != 7 {
		t.Fatalf("expected incoming+data, got %d sends", len(s.sent))
	}
	if !bytes.Equal(s.sent[5], []byte{MSG_SNDIN_DATA_INCOM}) {
		t.Fatalf("expected DATA_INCOMING, got %v", s.sent[5])
	}
	if !bytes.Equal(s.sent[6], []byte{MSG_SNDIN_DATA, 0x11, 0x22, 0x33, 0x44}) {
		t.Fatalf("bad data pdu: %v", s.sent[6])
	}
}

func TestOpenAcceptedWhenDisabled(t *testing.T) {
	h := NewHandler()
	s := &fakeSender{}
	h.Sender(s)
	// enabled stays false: OPEN is still accepted (send-gate only),
	// so arming later works without reconnect.

	body := []byte{MSG_SNDIN_FORMATS}
	hdr := make([]byte, 8)
	binary.LittleEndian.PutUint32(hdr[0:], 1)
	binary.LittleEndian.PutUint32(hdr[4:], 8+18)
	body = append(body, hdr...)
	body = append(body, waveFormatEx(1, 1, 48000, 16)...)
	h.Process(body)

	h.Process([]byte{MSG_SNDIN_OPEN, 1, 0, 0, 0, 0, 0, 0, 0})
	last := s.sent[len(s.sent)-1]
	if !bytes.Equal(last, []byte{MSG_SNDIN_OPEN_REPLY, 0, 0, 0, 0}) {
		t.Fatalf("expected OPEN_REPLY(0), got %v", last)
	}

	// Dropped frames produce no traffic until armed.
	n := len(s.sent)
	h.PushPCM([]byte{0x1, 0x2})
	if len(s.sent) != n {
		t.Fatalf("disabled push produced traffic")
	}

	// After arming, buffered-open channel streams immediately.
	h.SetEnabled(true)
	h.PushPCM([]byte{0x1, 0x2})
	if len(s.sent) != n+2 {
		t.Fatalf("armed push produced no traffic")
	}
}

// TestFramesPerPacketChunking: PCM режется ровно по FramesPerPacket
// (2 канала, 16 бит → 4 байта на кадр); остаток копится до следующего вызова.
func TestFramesPerPacketChunking(t *testing.T) {
	h := NewHandler()
	s := &fakeSender{}
	h.Sender(s)
	h.SetEnabled(true)

	body := []byte{MSG_SNDIN_FORMATS}
	hdr := make([]byte, 8)
	binary.LittleEndian.PutUint32(hdr[0:], 1)
	binary.LittleEndian.PutUint32(hdr[4:], 8+18)
	body = append(body, hdr...)
	body = append(body, waveFormatEx(1, 2, 44100, 16)...)
	h.Process(body)

	h.Process([]byte{MSG_SNDIN_OPEN, 2, 0, 0, 0, 0, 0, 0, 0}) // 2 кадра = 8 байт
	base := len(s.sent)

	// 10 байт → один пакет на 8 байт, остаток 2 байта копится.
	h.PushPCM([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
	if len(s.sent)-base != 2 {
		t.Fatalf("expected 1 chunk (2 pdus), got %d sends", len(s.sent)-base)
	}
	if !bytes.Equal(s.sent[base+1], append([]byte{MSG_SNDIN_DATA}, []byte{1, 2, 3, 4, 5, 6, 7, 8}...)) {
		t.Fatalf("bad chunk payload: %v", s.sent[base+1])
	}

	// Ещё 6 байт → из остатка+новых собирается второй полный пакет.
	base = len(s.sent)
	h.PushPCM([]byte{11, 12, 13, 14, 15, 16})
	if len(s.sent)-base != 2 {
		t.Fatalf("expected second chunk, got %d sends", len(s.sent)-base)
	}
	if !bytes.Equal(s.sent[base+1], append([]byte{MSG_SNDIN_DATA}, []byte{9, 10, 11, 12, 13, 14, 15, 16}...)) {
		t.Fatalf("bad second chunk payload: %v", s.sent[base+1])
	}
}

func TestVersionTooNewIgnored(t *testing.T) {
	h := NewHandler()
	s := &fakeSender{}
	h.Sender(s)
	h.Process([]byte{MSG_SNDIN_VERSION, 9, 0, 0, 0})
	if len(s.sent) != 0 {
		t.Fatalf("expected silence on newer version, got %v", s.sent)
	}
}
