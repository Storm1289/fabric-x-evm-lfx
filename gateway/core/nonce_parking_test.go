/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package core

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/hyperledger/fabric-x-evm/gateway/domain"
	"github.com/stretchr/testify/require"
)

// stubState is a mutable stateReader whose committed nonce per sender can be
// advanced between calls to simulate blocks committing.
type stubState struct {
	mu     sync.Mutex
	nonces map[common.Address]uint64
	err    error
}

func newStubState() *stubState {
	return &stubState{nonces: make(map[common.Address]uint64)}
}

func (s *stubState) NonceAt(_ context.Context, a common.Address, _ *big.Int) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return 0, s.err
	}
	return s.nonces[a], nil
}

func (s *stubState) set(a common.Address, n uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nonces[a] = n
}

// admitSink records the transactions the gate hands to the worker queue.
type admitSink struct {
	mu  sync.Mutex
	txs []*types.Transaction
}

func (a *admitSink) admit(tx *types.Transaction) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.txs = append(a.txs, tx)
}

func (a *admitSink) nonces() []uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]uint64, len(a.txs))
	for i, tx := range a.txs {
		out[i] = tx.Nonce()
	}
	return out
}

func newTestGate(state stateReader) (*nonceGate, *admitSink) {
	sink := &admitSink{}
	signer := types.LatestSignerForChainID(big.NewInt(testChainID))
	return newNonceGate(state, signer, sink.admit), sink
}

func senderAddr(key *ecdsa.PrivateKey) common.Address {
	return crypto.PubkeyToAddress(key.PublicKey)
}

// committedBlock builds the committed-transaction slice Released expects, only
// the FromAddress field is read.
func committedBlock(froms ...common.Address) []domain.Transaction {
	txs := make([]domain.Transaction, len(froms))
	for i, from := range froms {
		txs[i] = domain.Transaction{FromAddress: from.Bytes()}
	}
	return txs
}

func TestNonceGate_InOrderAdmitsImmediately(t *testing.T) {
	key := newKey(t)
	state := newStubState()
	state.set(senderAddr(key), 5)
	gate, sink := newTestGate(state)

	tx := newValidTx(t, key, validTxOpts{nonce: 5})
	require.NoError(t, gate.Admit(context.Background(), tx))

	require.Equal(t, []uint64{5}, sink.nonces())
	require.Nil(t, gate.IsPending(tx.Hash()))
}

func TestNonceGate_FutureNonceParks(t *testing.T) {
	key := newKey(t)
	state := newStubState()
	state.set(senderAddr(key), 5)
	gate, sink := newTestGate(state)

	tx := newValidTx(t, key, validTxOpts{nonce: 6})
	require.NoError(t, gate.Admit(context.Background(), tx))

	require.Empty(t, sink.nonces())
	require.Equal(t, tx.Hash(), gate.IsPending(tx.Hash()).Hash())
	require.Nil(t, gate.IsPending(common.Hash{0xde, 0xad}))
}

func TestNonceGate_ReleaseOnCommit(t *testing.T) {
	key := newKey(t)
	from := senderAddr(key)
	state := newStubState()
	state.set(from, 5)
	gate, sink := newTestGate(state)

	future := newValidTx(t, key, validTxOpts{nonce: 6})
	require.NoError(t, gate.Admit(context.Background(), future))
	ready := newValidTx(t, key, validTxOpts{nonce: 5})
	require.NoError(t, gate.Admit(context.Background(), ready))
	require.Equal(t, []uint64{5}, sink.nonces())

	// Nonce 5 commits; the sender's nonce advances to 6.
	state.set(from, 6)
	gate.Released(context.Background(), committedBlock(from))

	require.Equal(t, []uint64{5, 6}, sink.nonces())
	require.Nil(t, gate.IsPending(future.Hash()))
}

func TestNonceGate_ReleasesChainOnePerCommit(t *testing.T) {
	key := newKey(t)
	from := senderAddr(key)
	state := newStubState()
	state.set(from, 5)
	gate, sink := newTestGate(state)

	tx6 := newValidTx(t, key, validTxOpts{nonce: 6})
	tx7 := newValidTx(t, key, validTxOpts{nonce: 7})
	require.NoError(t, gate.Admit(context.Background(), tx6))
	require.NoError(t, gate.Admit(context.Background(), tx7))
	require.NoError(t, gate.Admit(context.Background(), newValidTx(t, key, validTxOpts{nonce: 5})))
	require.Equal(t, []uint64{5}, sink.nonces())

	// Only nonce 6 is released while 7 still has a gap.
	state.set(from, 6)
	gate.Released(context.Background(), committedBlock(from))
	require.Equal(t, []uint64{5, 6}, sink.nonces())
	require.Equal(t, tx7.Hash(), gate.IsPending(tx7.Hash()).Hash())

	// Nonce 6 commits, releasing 7.
	state.set(from, 7)
	gate.Released(context.Background(), committedBlock(from))
	require.Equal(t, []uint64{5, 6, 7}, sink.nonces())
	require.Nil(t, gate.IsPending(tx7.Hash()))
}

func TestNonceGate_TwoSendersIndependent(t *testing.T) {
	keyA, keyB := newKey(t), newKey(t)
	fromA, fromB := senderAddr(keyA), senderAddr(keyB)
	state := newStubState()
	state.set(fromA, 5)
	state.set(fromB, 2)
	gate, sink := newTestGate(state)

	txA := newValidTx(t, keyA, validTxOpts{nonce: 6})
	txB := newValidTx(t, keyB, validTxOpts{nonce: 2})
	require.NoError(t, gate.Admit(context.Background(), txA)) // parked
	require.NoError(t, gate.Admit(context.Background(), txB)) // in order

	require.Equal(t, []uint64{2}, sink.nonces())
	require.Equal(t, txA.Hash(), gate.IsPending(txA.Hash()).Hash())
}

func TestNonceGate_PerSenderCap(t *testing.T) {
	key := newKey(t)
	state := newStubState()
	state.set(senderAddr(key), 5)
	gate, _ := newTestGate(state)
	gate.maxPerSender = 2

	require.NoError(t, gate.Admit(context.Background(), newValidTx(t, key, validTxOpts{nonce: 6})))
	require.NoError(t, gate.Admit(context.Background(), newValidTx(t, key, validTxOpts{nonce: 7})))
	err := gate.Admit(context.Background(), newValidTx(t, key, validTxOpts{nonce: 8}))
	require.ErrorIs(t, err, errTooManyParked)
}

func TestNonceGate_DuplicateNonceDoesNotCountTwice(t *testing.T) {
	key := newKey(t)
	state := newStubState()
	state.set(senderAddr(key), 5)
	gate, _ := newTestGate(state)
	gate.maxPerSender = 1

	require.NoError(t, gate.Admit(context.Background(), newValidTx(t, key, validTxOpts{nonce: 6})))
	// Re-parking the same nonce replaces, so it must not trip the cap.
	require.NoError(t, gate.Admit(context.Background(), newValidTx(t, key, validTxOpts{nonce: 6, data: []byte{0x01}})))
	// A different future nonce exceeds the cap of 1.
	require.ErrorIs(t, gate.Admit(context.Background(), newValidTx(t, key, validTxOpts{nonce: 7})), errTooManyParked)
}

func TestNonceGate_TTLEviction(t *testing.T) {
	key := newKey(t)
	state := newStubState()
	state.set(senderAddr(key), 5)
	gate, _ := newTestGate(state)

	now := time.Now()
	gate.now = func() time.Time { return now }

	stale := newValidTx(t, key, validTxOpts{nonce: 6})
	require.NoError(t, gate.Admit(context.Background(), stale))

	// Advance past the TTL; the next park sweeps the expired entry.
	now = now.Add(defaultParkedTTL + time.Second)
	fresh := newValidTx(t, key, validTxOpts{nonce: 7})
	require.NoError(t, gate.Admit(context.Background(), fresh))

	require.Nil(t, gate.IsPending(stale.Hash()))
	require.Equal(t, fresh.Hash(), gate.IsPending(fresh.Hash()).Hash())
}

func TestNonceGate_StaleParkedDroppedOnRelease(t *testing.T) {
	key := newKey(t)
	from := senderAddr(key)
	state := newStubState()
	state.set(from, 5)
	gate, sink := newTestGate(state)

	parked := newValidTx(t, key, validTxOpts{nonce: 6})
	require.NoError(t, gate.Admit(context.Background(), parked))

	// The sender's nonce jumps past the parked tx; it is dropped, none admitted.
	state.set(from, 8)
	gate.Released(context.Background(), committedBlock(from))

	require.Empty(t, sink.nonces())
	require.Nil(t, gate.IsPending(parked.Hash()))
}

func TestNonceGate_StateErrorPropagates(t *testing.T) {
	key := newKey(t)
	state := newStubState()
	state.err = errors.New("ledger unavailable")
	gate, _ := newTestGate(state)

	err := gate.Admit(context.Background(), newValidTx(t, key, validTxOpts{nonce: 5}))
	require.ErrorIs(t, err, state.err)
}

func TestNonceGate_AdmitSenderRecoverError(t *testing.T) {
	key := newKey(t)
	gate, _ := newTestGate(newStubState())

	// Signed for a different chain id, so the gate's signer cannot recover the sender.
	to := common.HexToAddress("0x1111111111111111111111111111111111111111")
	raw := types.NewTransaction(0, to, big.NewInt(0), 21_000, big.NewInt(1), nil)
	tx, err := types.SignTx(raw, types.NewEIP155Signer(big.NewInt(testChainID+1)), key)
	require.NoError(t, err)

	require.Error(t, gate.Admit(context.Background(), tx))
}

func TestNonceGate_ReleasedNonceLookupError(t *testing.T) {
	key := newKey(t)
	from := senderAddr(key)
	state := newStubState()
	state.set(from, 5)
	gate, sink := newTestGate(state)

	require.NoError(t, gate.Admit(context.Background(), newValidTx(t, key, validTxOpts{nonce: 6})))

	// A failing nonce lookup during release is logged and skipped, admitting nothing.
	state.err = errors.New("boom")
	gate.Released(context.Background(), committedBlock(from))
	require.Empty(t, sink.nonces())
}

func TestNonceGate_ReleaseSenderUntrackedNoop(t *testing.T) {
	gate, sink := newTestGate(newStubState())

	// No parked txs for this sender: releasing is a no-op.
	gate.releaseSender(common.HexToAddress("0x2222222222222222222222222222222222222222"), 0)
	require.Empty(t, sink.nonces())
}
