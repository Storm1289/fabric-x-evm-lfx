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

// Memory guardrails per sender.
const (
	defaultMaxParkedPerSender = 64
	defaultParkedTTL          = 3 * time.Minute
	// Cap on cached senders before LRU eviction.
	defaultMaxSenders = 1 << 20
)

var errTooManyParked = errors.New("too many queued (future-nonce) transactions for sender")

// enqueuer receives a ready transaction.
type enqueuer interface {
	Enqueue(tx *types.Transaction)
}

// nonceSequencer gates a sender's transactions by nonce.
type nonceSequencer interface {
	Admit(ctx context.Context, tx *types.Transaction) error
	Observe(committed []domain.Transaction)
	IsPending(hash common.Hash) *types.Transaction
}

// nonceGate enqueues each sender's next expected nonce, parks higher ones until
// the gap fills, and rejects lower ones. The next nonce is cached per sender.
type nonceGate struct {
	mu     sync.RWMutex
	state  stateReader
	signer types.Signer
	queue  enqueuer

	senders map[common.Address]*senderState

	maxPerSender int
	maxSenders   int
	ttl          time.Duration
	now          func() time.Time
}

// senderState is one sender's next expected nonce and its parked transactions.
type senderState struct {
	next     uint64 // next nonce eligible to admit
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
		maxSenders:   defaultMaxSenders,
		ttl:          defaultParkedTTL,
		now:          time.Now,
	}
}

// Admit enqueues tx at its expected nonce, parks a higher one, rejects a lower one.
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
		// First sight: seed the nonce from state.
		seed, err := g.state.NonceAt(ctx, from, nil)
		if err != nil {
			return fmt.Errorf("look up nonce: %w", err)
		}
		ss = &senderState{next: seed, parked: make(map[uint64]parkedTx)}
		g.senders[from] = ss
		seeded = true
	}
	ss.lastSeen = g.now()
	if seeded {
		g.evictLRU()
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

// resync updates the cache to the sender's committed nonce.
func (g *nonceGate) resync(ctx context.Context, tx *types.Transaction) error {
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
	ss := g.senders[from]
	if ss == nil {
		ss = &senderState{parked: make(map[uint64]parkedTx)}
		g.senders[from] = ss
	}
	ss.next = committed
	ss.lastSeen = g.now()
	return nil
}

// reconcilingGate re-reads the ledger nonce before each admit, following the
// out-of-band nonce changes (reverts, primed state) that only the test backend makes.
type reconcilingGate struct {
	*nonceGate
}

func (g reconcilingGate) Admit(ctx context.Context, tx *types.Transaction) error {
	if err := g.nonceGate.resync(ctx, tx); err != nil {
		return err
	}
	return g.nonceGate.Admit(ctx, tx)
}

// Observe advances each sender's cached nonce from the block and releases any
// now-ready parked transaction. Only Fabric-valid commits advance a nonce.
func (g *nonceGate) Observe(committed []domain.Transaction) {
	// Highest valid nonce per sender.
	highest := make(map[common.Address]uint64)
	for i := range committed {
		if committed[i].FabricTxStatus != 0 {
			continue // invalidated: nonce not consumed
		}
		tx := committed[i].ToEthTx()
		if tx == nil {
			continue
		}
		from := common.BytesToAddress(committed[i].FromAddress)
		if cur, ok := highest[from]; !ok || tx.Nonce() > cur {
			highest[from] = tx.Nonce()
		}
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	now := g.now()
	for from, n := range highest {
		ss := g.senders[from]
		if ss == nil {
			// Persist every committed sender, even one never admitted here.
			ss = &senderState{parked: make(map[uint64]parkedTx)}
			g.senders[from] = ss
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
	g.evictLRU()
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

// evictLRU drops least-recently-seen senders with no parked txs while over the cap.
func (g *nonceGate) evictLRU() {
	for len(g.senders) > g.maxSenders {
		var oldest common.Address
		var oldestSeen time.Time
		found := false
		for from, ss := range g.senders {
			if len(ss.parked) > 0 {
				continue
			}
			if !found || ss.lastSeen.Before(oldestSeen) {
				oldest, oldestSeen, found = from, ss.lastSeen, true
			}
		}
		if !found {
			return
		}
		delete(g.senders, oldest)
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
