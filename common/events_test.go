/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package common

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"github.com/hyperledger/fabric-x-sdk/state"
	"google.golang.org/protobuf/proto"
)

func TestUnmarshalEvents(t *testing.T) {
	tests := []struct {
		name    string
		logs    []state.Log
		wantErr bool
	}{
		{
			name: "empty input",
			logs: nil,
		},
		{
			name: "single log",
			logs: []state.Log{
				{
					Address: []byte{0x01, 0x02, 0x03},
					Topics:  [][]byte{{0x0a, 0x0b}, {0x0c, 0x0d}},
					Data:    []byte{0xff, 0xfe},
				},
			},
		},
		{
			name: "multiple logs",
			logs: []state.Log{
				{
					Address: []byte{0x01},
					Topics:  [][]byte{{0x0a}},
					Data:    []byte{0xff},
				},
				{
					Address: []byte{0x02},
					Topics:  [][]byte{{0x0b}, {0x0c}},
					Data:    []byte{0xee, 0xdd},
				},
			},
		},
		{
			name: "log with empty fields",
			logs: []state.Log{
				{
					Address: []byte{},
					Topics:  [][]byte{},
					Data:    []byte{},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// The endorser emits the JSON logs as the event bytes; the SDK
			// carries them through to the block unchanged.
			event, _ := json.Marshal(tt.logs)

			got, err := UnmarshalLogs(event)
			if (err != nil) != tt.wantErr {
				t.Fatalf("UnmarshalEvents() error = %v, wantErr %v", err, tt.wantErr)
			}

			// For nil/empty input, expect empty slice
			if tt.logs == nil {
				if len(got) != 0 {
					t.Errorf("expected empty slice, got %v", got)
				}
				return
			}

			if len(got) != len(tt.logs) {
				t.Fatalf("got %d logs, want %d", len(got), len(tt.logs))
			}

			for i := range tt.logs {
				if !bytes.Equal(got[i].Address, tt.logs[i].Address) {
					t.Errorf("log[%d].Address = %v, want %v", i, got[i].Address, tt.logs[i].Address)
				}
				if len(got[i].Topics) != len(tt.logs[i].Topics) {
					t.Errorf("log[%d].Topics length = %d, want %d", i, len(got[i].Topics), len(tt.logs[i].Topics))
				}
				for j := range tt.logs[i].Topics {
					if !bytes.Equal(got[i].Topics[j], tt.logs[i].Topics[j]) {
						t.Errorf("log[%d].Topics[%d] = %v, want %v", i, j, got[i].Topics[j], tt.logs[i].Topics[j])
					}
				}
				if !bytes.Equal(got[i].Data, tt.logs[i].Data) {
					t.Errorf("log[%d].Data = %v, want %v", i, got[i].Data, tt.logs[i].Data)
				}
			}
		})
	}
}

func TestUnmarshalEvents_InvalidInput(t *testing.T) {
	tests := []struct {
		name  string
		input []byte
	}{
		{
			name:  "not json",
			input: []byte{0xff, 0xff, 0xff},
		},
		{
			name:  "truncated json",
			input: []byte(`[{"Address":`),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := UnmarshalLogs(tt.input)
			if err == nil {
				t.Error("expected error for invalid input")
			}
		})
	}
}

// ---- UnmarshalLogs: empty and empty-payload branches ----

func TestUnmarshalLogs_EmptyInputReturnsEmptySlice(t *testing.T) {
	got, err := UnmarshalLogs(nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if got == nil || len(got) != 0 {
		t.Errorf("want empty slice, got %v", got)
	}
}

func TestUnmarshalLogs_EmptyJSONArrayReturnsEmptySlice(t *testing.T) {
	got, err := UnmarshalLogs([]byte("[]"))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if got == nil || len(got) != 0 {
		t.Errorf("want empty slice, got %v", got)
	}
}

// TestUnmarshalLogs_BothBackendShapes covers the one place the two protocols
// still differ. Fabric-X carries the endorser's event verbatim, while Fabric
// has no metadata field and the SDK's builder wraps it in a ChaincodeEvent
// named "log". Both must decode to the same logs.
func TestUnmarshalLogs_BothBackendShapes(t *testing.T) {
	want := []state.Log{{Address: []byte{0x01}, Topics: [][]byte{{0x0a}}, Data: []byte{0xff}}}
	fabricx, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("marshal setup: %v", err)
	}
	fabric, err := proto.Marshal(&peer.ChaincodeEvent{Payload: fabricx, EventName: "log"})
	if err != nil {
		t.Fatalf("marshal setup: %v", err)
	}

	for name, event := range map[string][]byte{"fabric-x": fabricx, "fabric": fabric} {
		t.Run(name, func(t *testing.T) {
			got, err := UnmarshalLogs(event)
			if err != nil {
				t.Fatalf("UnmarshalLogs: %v", err)
			}
			if len(got) != 1 || !bytes.Equal(got[0].Data, want[0].Data) {
				t.Errorf("got %+v, want %+v", got, want)
			}
		})
	}
}

// TestIsRevertEvent_BothBackendShapes is the same split for revert detection:
// a revert must be recognised whether or not Fabric's wrapper is around it.
func TestIsRevertEvent_BothBackendShapes(t *testing.T) {
	fabricx, err := MarshalRevert([]byte("boom"), "cc", "tx-1")
	if err != nil {
		t.Fatalf("MarshalRevert: %v", err)
	}
	fabric, err := proto.Marshal(&peer.ChaincodeEvent{Payload: fabricx, EventName: "log"})
	if err != nil {
		t.Fatalf("marshal setup: %v", err)
	}

	for name, event := range map[string][]byte{"fabric-x": fabricx, "fabric": fabric} {
		t.Run(name, func(t *testing.T) {
			if !IsRevertEvent(event) {
				t.Error("expected revert event detected")
			}
		})
	}
}

// ---- MarshalRevert ----

func TestMarshalRevert_SetsEventNameWithTxID(t *testing.T) {
	txID := "tx-abc"
	payload := []byte("revert-data")
	out, err := MarshalRevert(payload, "cc", txID)
	if err != nil {
		t.Fatalf("MarshalRevert err: %v", err)
	}
	var got peer.ChaincodeEvent
	if err := proto.Unmarshal(out, &got); err != nil {
		t.Fatalf("unmarshal roundtrip: %v", err)
	}
	if got.EventName != "revert:"+txID {
		t.Errorf("EventName = %q, want %q", got.EventName, "revert:"+txID)
	}
	if !strings.HasPrefix(got.EventName, "revert:") {
		t.Errorf("EventName missing revert: prefix")
	}
	if !bytes.Equal(got.Payload, payload) {
		t.Errorf("Payload = %x, want %x", got.Payload, payload)
	}
	if got.TxId != txID {
		t.Errorf("TxId = %q, want %q", got.TxId, txID)
	}
}

// ---- IsRevertEvent ----

func TestIsRevertEvent_EmptyReturnsFalse(t *testing.T) {
	if IsRevertEvent(nil) {
		t.Error("nil should be false")
	}
	if IsRevertEvent([]byte{}) {
		t.Error("empty slice should be false")
	}
}

func TestIsRevertEvent_BadProtoReturnsFalse(t *testing.T) {
	if IsRevertEvent([]byte{0xff, 0xff, 0xff}) {
		t.Error("garbage bytes should be false")
	}
}

// TestIsRevertEvent_SuccessLogsReturnFalse is the regression guard for the
// failure this change fixes. A successful transaction carries JSON logs in the
// event field, not a ChaincodeEvent. If the unwrapping is off by a layer this
// reports false for a genuine revert too, and a revert mines as a success -
// silently, because nothing errors.
func TestIsRevertEvent_SuccessLogsReturnFalse(t *testing.T) {
	logs, err := json.Marshal([]state.Log{{Address: []byte{0x01}, Data: []byte{0xff}}})
	if err != nil {
		t.Fatalf("marshal setup: %v", err)
	}
	if IsRevertEvent(logs) {
		t.Error("JSON logs must not be detected as a revert")
	}
	if IsExecFailureEvent(logs) {
		t.Error("JSON logs must not be detected as an exec failure")
	}
}

func TestIsRevertEvent_WithRevertPrefixReturnsTrue(t *testing.T) {
	// MarshalRevert's output reaches the block as-is.
	event, err := MarshalRevert([]byte("payload"), "cc", "tx-1")
	if err != nil {
		t.Fatalf("MarshalRevert: %v", err)
	}
	if !IsRevertEvent(event) {
		t.Error("expected revert event detected")
	}
}

func TestIsRevertEvent_WithoutRevertPrefixReturnsFalse(t *testing.T) {
	// EventName is "log", not "revert:*".
	event, err := proto.Marshal(&peer.ChaincodeEvent{Payload: []byte("x"), EventName: "log"})
	if err != nil {
		t.Fatalf("marshal setup: %v", err)
	}
	if IsRevertEvent(event) {
		t.Error("non-revert event should be false")
	}
}

// ---- MarshalExecFailure / IsExecFailureEvent ----

func TestMarshalExecFailure_SetsEventNameWithTxID(t *testing.T) {
	txID := "tx-abc"
	payload := []byte("")
	out, err := MarshalExecFailure(payload, "cc", txID)
	if err != nil {
		t.Fatalf("MarshalExecFailure err: %v", err)
	}
	var got peer.ChaincodeEvent
	if err := proto.Unmarshal(out, &got); err != nil {
		t.Fatalf("unmarshal roundtrip: %v", err)
	}
	if got.EventName != "execfail:"+txID {
		t.Errorf("EventName = %q, want %q", got.EventName, "execfail:"+txID)
	}
	if got.TxId != txID {
		t.Errorf("TxId = %q, want %q", got.TxId, txID)
	}
}

func TestIsExecFailureEvent_EmptyReturnsFalse(t *testing.T) {
	if IsExecFailureEvent(nil) {
		t.Error("nil should be false")
	}
	if IsExecFailureEvent([]byte{}) {
		t.Error("empty slice should be false")
	}
}

func TestIsExecFailureEvent_WithExecFailurePrefixReturnsTrue(t *testing.T) {
	event, err := MarshalExecFailure(nil, "cc", "tx-1")
	if err != nil {
		t.Fatalf("MarshalExecFailure: %v", err)
	}
	if !IsExecFailureEvent(event) {
		t.Error("expected exec-failure event detected")
	}
}

func TestIsExecFailureEvent_RevertEventReturnsFalse(t *testing.T) {
	event, err := MarshalRevert([]byte("payload"), "cc", "tx-1")
	if err != nil {
		t.Fatalf("MarshalRevert: %v", err)
	}
	if IsExecFailureEvent(event) {
		t.Error("revert event should not be an exec-failure event")
	}
}

func TestIsRevertEvent_ExecFailureEventReturnsFalse(t *testing.T) {
	inner, err := MarshalExecFailure(nil, "cc", "tx-1")
	if err != nil {
		t.Fatalf("MarshalExecFailure: %v", err)
	}
	outer, err := proto.Marshal(&peer.ChaincodeEvent{Payload: inner, EventName: "log"})
	if err != nil {
		t.Fatalf("outer marshal: %v", err)
	}
	if IsRevertEvent(outer) {
		t.Error("exec-failure event should not be a revert event")
	}
}
