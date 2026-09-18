package drdynvc

import (
	"bytes"
	"encoding/binary"
	"testing"
)

type fakeHandler struct{}

func (f *fakeHandler) Process(data []byte) {}

type fakeSender struct {
	sent [][]byte
}

func (f *fakeSender) SendToChannel(channel string, s []byte) (int, error) {
	cp := make([]byte, len(s))
	copy(cp, s)
	f.sent = append(f.sent, cp)
	return len(s), nil
}

// TestSendDvcDataSmall: порция, влезающая в CHANNEL_CHUNK_LENGTH, уходит
// одним Data PDU.
func TestSendDvcDataSmall(t *testing.T) {
	c := NewDvcClient()
	fs := &fakeSender{}
	c.Sender(fs)
	c.RegisterHandler("AUDIO_INPUT", &fakeHandler{})
	c.bindChannel(1, "AUDIO_INPUT", 0)

	c.SendDvcData(1, []byte{1, 2, 3, 4})
	if len(fs.sent) != 1 {
		t.Fatalf("expected 1 pdu, got %d", len(fs.sent))
	}
	got := fs.sent[0]
	if cmd := got[0] >> 4; cmd != DYNVC_DATA {
		t.Fatalf("expected DATA (0x03), got 0x%x", cmd)
	}
	if !bytes.Equal(got[2:], []byte{1, 2, 3, 4}) {
		t.Fatalf("bad payload: %v", got)
	}
}

// TestSendDvcDataFragmented: 1765 байт (реальный аудиопакет 441 кадр ×
// 2 канала × 16 бит + заголовок) режется на Data First + Data, каждая
// порция ≤ CHANNEL_CHUNK_LENGTH, а при сборке даёт исходные данные.
func TestSendDvcDataFragmented(t *testing.T) {
	c := NewDvcClient()
	fs := &fakeSender{}
	c.Sender(fs)
	c.RegisterHandler("AUDIO_INPUT", &fakeHandler{})
	c.bindChannel(3, "AUDIO_INPUT", 1) // 2-байтовый ChannelId

	payload := make([]byte, 1765)
	for i := range payload {
		payload[i] = byte(i)
	}
	c.SendDvcData(3, payload)

	if len(fs.sent) < 2 {
		t.Fatalf("expected fragmentation, got %d pdus", len(fs.sent))
	}
	// Первый PDU — Data First с полной длиной и Sp=cb.
	first := fs.sent[0]
	if cmd := first[0] >> 4; cmd != DYNVC_DATA_FIRST {
		t.Fatalf("first pdu must be DATA_FIRST, got 0x%x", cmd)
	}
	if sp := (first[0] >> 2) & 0x3; sp != 1 {
		t.Fatalf("expected Sp=1 (2-byte length) for 1765 bytes, got %d", sp)
	}
	total := binary.LittleEndian.Uint16(first[3:5])
	if total != uint16(len(payload)) {
		t.Fatalf("bad total length: %d", total)
	}

	var reassembled []byte
	for i, pdu := range fs.sent {
		if len(pdu) > 1600 {
			t.Fatalf("pdu %d exceeds CHANNEL_CHUNK_LENGTH: %d", i, len(pdu))
		}
		cmd := pdu[0] >> 4
		idLen := dvcIdLen((pdu[0] >> 0) & 0x3)
		offset := 1 + idLen
		if i == 0 {
			if cmd != DYNVC_DATA_FIRST {
				t.Fatalf("pdu 0: expected DATA_FIRST")
			}
			offset += 2 // поле длины (Sp=1)
		} else if cmd != DYNVC_DATA {
			t.Fatalf("pdu %d: expected DATA, got 0x%x", i, cmd)
		}
		reassembled = append(reassembled, pdu[offset:]...)
	}
	if !bytes.Equal(reassembled, payload) {
		t.Fatalf("reassembled %d bytes, want %d", len(reassembled), len(payload))
	}
}
