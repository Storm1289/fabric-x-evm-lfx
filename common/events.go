/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package common

import (
	"encoding/json"
	"strings"

	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"github.com/hyperledger/fabric-x-sdk/state"
	"google.golang.org/protobuf/proto"
)

// endorserEvent returns the bytes the endorser put in ExecutionResult.Event,
// which is not quite what blocks.Transaction.Events carries on both backends.
//
// Fabric-X keeps the event in the transaction metadata and the SDK hands it
// back verbatim. Fabric has no such field: ChaincodeAction.Events is by
// definition a marshalled ChaincodeEvent, so the SDK's Fabric builder wraps the
// endorser's event in one named "log" and the parser returns the wrapper. This
// strips that wrapper when it is there, so the rest of this file sees one shape
// on both backends.
//
// The endorser never names an event "log" itself - MarshalRevert and
// MarshalExecFailure both use a "<kind>:<txID>" name, and a successful
// transaction carries JSON rather than a ChaincodeEvent - so the name is an
// unambiguous marker for the Fabric wrapper.
func endorserEvent(event []byte) []byte {
	var wrapper peer.ChaincodeEvent
	if err := proto.Unmarshal(event, &wrapper); err != nil {
		return event
	}
	if wrapper.EventName != "log" {
		return event
	}
	return wrapper.Payload
}

// UnmarshalLogs converts a transaction's event bytes back to a list of logs:
// on the success path the endorser emits the JSON-marshaled logs.
//
// Callers must only reach this for a status-1 transaction; a revert carries a
// ChaincodeEvent here instead (see eventName).
func UnmarshalLogs(event []byte) ([]state.Log, error) {
	if len(event) == 0 {
		return []state.Log{}, nil
	}

	payload := endorserEvent(event)
	if len(payload) == 0 {
		return []state.Log{}, nil
	}

	var logs []state.Log
	if err := json.Unmarshal(payload, &logs); err != nil {
		return nil, err
	}

	return logs, nil
}

// eventNameRevertPrefix is the prefix of the ChaincodeEvent name used
// to signal an EVM revert. The Fabric txid is appended so the full name is
// unique per transaction and cannot be forged by an EVM contract.
const eventNameRevertPrefix = "revert:"

// eventNameExecFailurePrefix marks a valid tx whose EVM execution otherwise
// faulted (out of gas, invalid opcode, ...) - mined like a revert, but with
// no ABI-encoded reason to carry.
const eventNameExecFailurePrefix = "execfail:"

// MarshalRevert wraps the raw revert payload in a ChaincodeEvent whose name
// is "revert:<txID>" so the committer can detect the revert and the marker
// cannot collide with any name an EVM contract could produce.
func MarshalRevert(payload []byte, namespace, txID string) ([]byte, error) {
	return proto.Marshal(&peer.ChaincodeEvent{
		Payload:     payload,
		ChaincodeId: namespace,
		TxId:        txID,
		EventName:   eventNameRevertPrefix + txID,
	})
}

// MarshalExecFailure wraps the raw EVM return data (typically empty) in a
// ChaincodeEvent whose name is "execfail:<txID>", the same shape MarshalRevert
// uses for a revert.
func MarshalExecFailure(payload []byte, namespace, txID string) ([]byte, error) {
	return proto.Marshal(&peer.ChaincodeEvent{
		Payload:     payload,
		ChaincodeId: namespace,
		TxId:        txID,
		EventName:   eventNameExecFailurePrefix + txID,
	})
}

// eventName returns the name of the ChaincodeEvent carried in a transaction's
// event bytes, or ok=false if the bytes are not one.
//
// endorserEvent normalises away Fabric's extra wrapper first, so what is left
// is the event MarshalRevert or MarshalExecFailure built. On the success path
// those bytes are JSON logs and not a ChaincodeEvent at all, which is what
// ok=false reports.
func eventName(event []byte) (name string, ok bool) {
	if len(event) == 0 {
		return "", false
	}
	var ev peer.ChaincodeEvent
	if err := proto.Unmarshal(endorserEvent(event), &ev); err != nil {
		return "", false
	}
	return ev.EventName, true
}

// IsRevertEvent reports whether the given event bytes represent an EVM revert.
func IsRevertEvent(event []byte) bool {
	name, ok := eventName(event)
	return ok && strings.HasPrefix(name, eventNameRevertPrefix)
}

// IsExecFailureEvent reports whether the given event bytes represent a valid
// tx whose EVM execution faulted without reverting (out of gas, invalid
// opcode, ...).
func IsExecFailureEvent(event []byte) bool {
	name, ok := eventName(event)
	return ok && strings.HasPrefix(name, eventNameExecFailurePrefix)
}
