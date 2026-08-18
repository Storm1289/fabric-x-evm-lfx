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
	ethcore "github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/hyperledger/fabric-x-evm/gateway/domain"
)

// Per-sender guardrails so one account with a nonce gap cannot exhaust memory.
const (
	defaultMaxParkedPerSender = 64
	defaultParkedTTL          = 3 * time.Minute
)

var errTooManyParked = errors.New("too many queued (future-nonce) transactions for sender")

// enqueuer receives a transaction that is ready for processing.
type enqueuer interface {
	Enqueue(tx *types.Transaction)
}

// nonceGate sequences a sender's transactions: it enqueues the next expected
// nonce, parks higher nonces until the gap fills, and rejects lower ones. Each
// sender's next nonce is cached in memory - seeded from state and advanced as
// blocks commit - so in-order admits do no state reads; a tx ahead of the cache
// re-reads once in case the cache lagged the ledger.
type nonceGate struct {
	mu     sync.RWMutex
	state  stateReader
	signer types.Signer
	queue  enqueuer

	senders map[common.Address]*senderState

	maxPerSender int
	ttl          time.Duration
	now          func() time.Time
}

// senderState is one sender's next expected nonce and its parked transactions.
type senderState struct {
	next     uint64 // next nonce eligible to admit (the ledger's committed nonce)
	parked   map[uint64]parkedTx
	lastSeen time.Time
}

type parkedTx struct {
	tx       *types.Transaction
	parkedAt time.Time
}

func newNonceGate(state stateReader, signer types.Signer, queue enqueuer) *nonceGate {
	return &nonceGate{
		state:        state,
		signer:       signer,
		queue:        queue,
		senders:      make(map[common.Address]*senderState),
		maxPerSender: defaultMaxParkedPerSender,
		ttl:          defaultParkedTTL,
		now:          time.Now,
	}
}

// Admit enqueues tx if it is the sender's next nonce, parks it if it is ahead,
// or rejects it as too low. It owns the nonce check; ValidateTx no longer does it.
func (g *nonceGate) Admit(ctx context.Context, tx *types.Transaction) error {
	from, err := types.Sender(g.signer, tx)
	if err != nil {
		return fmt.Errorf("recover sender: %w", err)
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	ss := g.senders[from]
	seeded := false
	if ss == nil {
		// First transaction from this sender: seed the next nonce from state.
		seed, err := g.state.NonceAt(ctx, from, nil)
		if err != nil {
			return fmt.Errorf("look up nonce: %w", err)
		}
		ss = &senderState{next: seed, parked: make(map[uint64]parkedTx)}
		g.senders[from] = ss
		seeded = true
	}
	ss.lastSeen = g.now()

	// A tx ahead of a cached next may just mean the cache lagged the ledger (a
	// restart, a missed commit). Re-read once before treating it as a gap.
	if !seeded && tx.Nonce() > ss.next {
		committed, err := g.state.NonceAt(ctx, from, nil)
		if err != nil {
			return fmt.Errorf("look up nonce: %w", err)
		}
		if committed > ss.next {
			ss.next = committed
		}
	}

	switch {
	case tx.Nonce() < ss.next:
		return fmt.Errorf("%w: next nonce %d, tx nonce %d", ethcore.ErrNonceTooLow, ss.next, tx.Nonce())
	case tx.Nonce() == ss.next:
		g.queue.Enqueue(tx)
		return nil
	}

	// Future nonce: park until the gap fills.
	ss.evictExpired(g.now(), g.ttl)
	if _, replacing := ss.parked[tx.Nonce()]; !replacing && len(ss.parked) >= g.maxPerSender {
		return errTooManyParked
	}
	ss.parked[tx.Nonce()] = parkedTx{tx: tx, parkedAt: g.now()}
	return nil
}

// Observe advances each committed sender's next nonce from the block and admits
// the now-ready parked transaction, if any. It does no state reads.
func (g *nonceGate) Observe(committed []domain.Transaction) {
	// Highest committed nonce per sender.
	highest := make(map[common.Address]uint64)
	seen := make(map[common.Address]bool)
	for i := range committed {
		tx := committed[i].ToEthTx()
		if tx == nil {
			continue
		}
		from := common.BytesToAddress(committed[i].FromAddress)
		if n := tx.Nonce(); !seen[from] || n > highest[from] {
			highest[from] = n
			seen[from] = true
		}
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	now := g.now()
	for from, n := range highest {
		ss := g.senders[from]
		if ss == nil {
			continue
		}
		if n+1 > ss.next {
			ss.next = n + 1
		}
		ss.lastSeen = now
		for nonce := range ss.parked {
			if nonce < ss.next {
				delete(ss.parked, nonce)
			}
		}
		if p, ok := ss.parked[ss.next]; ok {
			delete(ss.parked, ss.next)
			g.queue.Enqueue(p.tx)
		}
	}
	g.evictIdle(now)
}

// IsPending returns a parked transaction by hash, or nil if it is not parked.
func (g *nonceGate) IsPending(hash common.Hash) *types.Transaction {
	g.mu.RLock()
	defer g.mu.RUnlock()

	for _, ss := range g.senders {
		for _, p := range ss.parked {
			if p.tx.Hash() == hash {
				return p.tx
			}
		}
	}
	return nil
}

// evictIdle drops cached senders with no parked txs that have been idle past the
// TTL, bounding the map for one-shot senders.
func (g *nonceGate) evictIdle(now time.Time) {
	for from, ss := range g.senders {
		if len(ss.parked) == 0 && now.Sub(ss.lastSeen) > g.ttl {
			delete(g.senders, from)
		}
	}
}

// evictExpired drops parked transactions whose gap never filled within the TTL.
func (ss *senderState) evictExpired(now time.Time, ttl time.Duration) {
	for nonce, p := range ss.parked {
		if now.Sub(p.parkedAt) > ttl {
			delete(ss.parked, nonce)
		}
	}
}
