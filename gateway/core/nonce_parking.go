/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package core

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/hyperledger/fabric-x-evm/gateway/domain"
)

// Guardrails on parked (future-nonce) transactions, kept per sender so one
// account with a nonce gap cannot exhaust memory.
const (
	defaultMaxParkedPerSender = 64
	defaultParkedTTL          = 3 * time.Minute
)

// errTooManyParked is returned when a sender exceeds its parked-transaction cap.
var errTooManyParked = errors.New("too many queued (future-nonce) transactions for sender")

// nonceGate holds transactions whose nonce is ahead of the sender's committed
// nonce and admits them to the worker queue in nonce order as earlier nonces
// commit. It keeps a single transaction per sender in flight (issue #52);
// cross-sender dependency scheduling is handled separately (issue #59).
type nonceGate struct {
	mu     sync.Mutex
	state  stateReader
	signer types.Signer
	admit  func(*types.Transaction) // hands an in-order tx to the worker queue

	senders map[common.Address]*senderParking

	maxPerSender int
	ttl          time.Duration
	now          func() time.Time
}

// senderParking holds one sender's future-nonce transactions, keyed by nonce.
type senderParking struct {
	parked map[uint64]parkedTx
}

type parkedTx struct {
	tx       *types.Transaction
	parkedAt time.Time
}

// newNonceGate builds a gate that reads committed nonces from state and admits
// ready transactions via admit.
func newNonceGate(state stateReader, signer types.Signer, admit func(*types.Transaction)) *nonceGate {
	return &nonceGate{
		state:        state,
		signer:       signer,
		admit:        admit,
		senders:      make(map[common.Address]*senderParking),
		maxPerSender: defaultMaxParkedPerSender,
		ttl:          defaultParkedTTL,
		now:          time.Now,
	}
}

// Admit enqueues tx for processing when its nonce is the sender's next expected
// nonce, or parks it until the preceding nonces commit. Callers run ValidateTx
// (which rejects nonce-too-low) before Admit, so a non-future nonce is ready.
func (g *nonceGate) Admit(ctx context.Context, tx *types.Transaction) error {
	from, err := types.Sender(g.signer, tx)
	if err != nil {
		return fmt.Errorf("recover sender: %w", err)
	}

	committed, err := g.state.NonceAt(ctx, from, nil)
	if err != nil {
		return fmt.Errorf("look up nonce: %w", err)
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	// In order (or, defensively, stale): hand straight to the worker queue.
	if tx.Nonce() <= committed {
		g.admit(tx)
		return nil
	}

	// Future nonce: park until the gap fills.
	sp := g.senders[from]
	if sp == nil {
		sp = &senderParking{parked: make(map[uint64]parkedTx)}
		g.senders[from] = sp
	}
	sp.evictExpired(g.now(), g.ttl)

	if _, replacing := sp.parked[tx.Nonce()]; !replacing && len(sp.parked) >= g.maxPerSender {
		return errTooManyParked
	}
	sp.parked[tx.Nonce()] = parkedTx{tx: tx, parkedAt: g.now()}
	return nil
}

// Released is registered with the block-commit path. For each sender that both
// appears in the committed block and has parked transactions, it re-reads the
// committed nonce and admits the next in-order transaction if it is parked.
func (g *nonceGate) Released(ctx context.Context, committed []domain.Transaction) {
	g.mu.Lock()
	affected := make(map[common.Address]struct{})
	for i := range committed {
		from := common.BytesToAddress(committed[i].FromAddress)
		if _, tracked := g.senders[from]; tracked {
			affected[from] = struct{}{}
		}
	}
	g.mu.Unlock()

	// Read state outside the lock; it reflects the block just persisted.
	for from := range affected {
		nonce, err := g.state.NonceAt(ctx, from, nil)
		if err != nil {
			logger.Errorf("nonce gate: look up nonce for %s: %v", from.Hex(), err)
			continue
		}
		g.releaseSender(from, nonce)
	}
}

// releaseSender drops parked transactions below committed and admits the one at
// the committed nonce, if present.
func (g *nonceGate) releaseSender(from common.Address, committed uint64) {
	g.mu.Lock()
	defer g.mu.Unlock()

	sp := g.senders[from]
	if sp == nil {
		return
	}
	sp.evictExpired(g.now(), g.ttl)

	for nonce := range sp.parked {
		if nonce < committed {
			delete(sp.parked, nonce)
		}
	}

	if p, ok := sp.parked[committed]; ok {
		delete(sp.parked, committed)
		g.admit(p.tx)
	}

	if len(sp.parked) == 0 {
		delete(g.senders, from)
	}
}

// IsPending returns a parked transaction by hash, or nil if it is not parked.
func (g *nonceGate) IsPending(hash common.Hash) *types.Transaction {
	g.mu.Lock()
	defer g.mu.Unlock()

	for _, sp := range g.senders {
		for _, p := range sp.parked {
			if p.tx.Hash() == hash {
				return p.tx
			}
		}
	}
	return nil
}

// evictExpired drops parked transactions whose gap never filled within the TTL.
func (sp *senderParking) evictExpired(now time.Time, ttl time.Duration) {
	for nonce, p := range sp.parked {
		if now.Sub(p.parkedAt) > ttl {
			delete(sp.parked, nonce)
		}
	}
}
