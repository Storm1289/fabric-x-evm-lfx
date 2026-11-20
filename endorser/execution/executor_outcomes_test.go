/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package execution

import (
	"errors"
	"math/big"
	"testing"

	ethcommon "github.com/ethereum/go-ethereum/common"
	gethcore "github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/hyperledger/fabric-x-sdk/endorsement"
)

// requireRejected checks that err is a pre-execution rejection wrapping want, with no endorsement.
func requireRejected(t *testing.T, res endorsement.ExecutionResult, err error, want error) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected a rejection, got an endorsement with status %d", res.Status)
	}
	if _, ok := errors.AsType[*TxRejected](err); !ok {
		t.Errorf("error %v (%T) is not a *TxRejected", err, err)
	}
	if !errors.Is(err, want) {
		t.Errorf("error %v does not wrap %v", err, want)
	}
	if res.Status != 0 || len(res.Payload) != 0 || len(res.RWS.Writes) != 0 {
		t.Errorf("rejected tx produced a result: status=%d payload=%x writes=%d", res.Status, res.Payload, len(res.RWS.Writes))
	}
}

// A nonce ahead of the ledger is rejected before execution, not endorsed.
func TestEVMEngineExecute_NonceTooHighIsRejected(t *testing.T) {
	eng := newTestEngine(newTestBackend(t, "exec_nonce_high"))
	tx := signTestTx(t, types.NewTransaction(5, ethcommon.HexToAddress("0xdead"), big.NewInt(0), 21_000, big.NewInt(1), nil))

	res, err := eng.Execute(t.Context(), tx, 1_700_000_000)
	requireRejected(t, res, err, gethcore.ErrNonceTooHigh)
}

// A gas limit below the intrinsic cost is rejected by ApplyMessage, not endorsed.
func TestEVMEngineExecute_IntrinsicGasTooLowIsRejected(t *testing.T) {
	eng := newTestEngine(newTestBackend(t, "exec_intrinsic_gas"))
	tx := signTestTx(t, types.NewTransaction(0, ethcommon.HexToAddress("0xdead"), big.NewInt(0), 1_000, big.NewInt(1), nil))

	res, err := eng.Execute(t.Context(), tx, 1_700_000_000)
	requireRejected(t, res, err, gethcore.ErrIntrinsicGas)
}

// A tx signed for another chain is rejected when its sender cannot be recovered.
func TestEVMEngineExecute_WrongChainSignatureIsRejected(t *testing.T) {
	eng := newTestEngine(newTestBackend(t, "exec_wrong_chain"))
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	tx, err := types.SignTx(types.NewTransaction(0, ethcommon.HexToAddress("0xdead"), big.NewInt(0), 21_000, big.NewInt(1), nil), types.NewEIP155Signer(big.NewInt(1)), key)
	if err != nil {
		t.Fatal(err)
	}

	res, err := eng.Execute(t.Context(), tx, 1_700_000_000)
	requireRejected(t, res, err, types.ErrInvalidChainId)
}

type failingSnapshotter struct{ err error }

func (f failingSnapshotter) NewSnapshot(*uint64) (ReadStore, error) { return nil, f.err }

// A state snapshot that cannot be opened fails the call with that error, not as a rejection.
func TestEVMEngineExecute_SnapshotFailureIsReturned(t *testing.T) {
	want := errors.New("snapshot unavailable")
	eng := NewEVMEngine(Namespace, failingSnapshotter{err: want}, newTestEngine(nil).evmConfig, false)
	tx := signTestTx(t, types.NewTransaction(0, ethcommon.HexToAddress("0xdead"), big.NewInt(0), 21_000, big.NewInt(1), nil))

	res, err := eng.Execute(t.Context(), tx, 1_700_000_000)
	if !errors.Is(err, want) {
		t.Fatalf("error = %v, want %v", err, want)
	}
	if _, ok := errors.AsType[*TxRejected](err); ok {
		t.Error("snapshot failure reported as a tx rejection")
	}
	if res.Status != 0 || len(res.Payload) != 0 {
		t.Errorf("failed call produced a result: status=%d payload=%x", res.Status, res.Payload)
	}
}
