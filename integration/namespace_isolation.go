/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package integration

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum"
	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/params"

	"github.com/hyperledger/fabric-x-evm/gateway/core"
	"github.com/hyperledger/fabric-x-evm/gateway/testimpl/primer"
	"github.com/hyperledger/fabric-x-evm/integration/contracts"
)

// This file holds the backend-independent half of the namespace-isolation
// matrix; the per-backend entry points are in namespace_isolation_test.go.
// Two gateways, one namespace each, commit to the same channel, and both are
// then read over the Ethereum RPC surface. Every backend has to satisfy:
//
//   - No gateway returns the other namespace's transactions, receipts or logs,
//     and none loses its own.
//   - A block carrying a transaction from each namespace is reported by both
//     gateways as the same block, each listing only its own transaction.
//   - A block carrying nothing for a namespace is still published to that
//     namespace's gateway, empty.
//   - Block numbers are the channel's, not the namespace's: both gateways
//     report the same height, and it follows activity either of them caused.
//   - Every transaction is retrievable at the transactionIndex it reports for
//     itself, and no index ever yields the other namespace's transaction.
//
// transactionIndex is checked for that round trip only, not for a particular
// value. Today it is the position in the Fabric block and so can be sparse
// (see docs/COMPATIBILITY.md); #440 densifies it over the EVM transactions the
// gateway keeps. Both satisfy the round trip, so this suite does not have to
// change along with it.

// nsParty is one namespace and the gateway that commits to it.
type nsParty struct {
	// Namespace is the Fabric namespace this party's gateway is configured with.
	Namespace string
	// Gateway is the gateway serving that namespace.
	Gateway *core.Gateway
	// Eth is an in-process RPC client for Gateway, carrying the FilterAPI from
	// its handler chain when one was registered (see NewNativeEthClient).
	Eth *ethclient.Client
	// Contract is the EVM contract deployed on this namespace, and the signer
	// that owns it. Each party gets its own, so a leak is unambiguous: an
	// address or tx hash from one party can never legitimately appear in the
	// other's answers.
	Contract ethcommon.Address
	Client   *EthClient
}

// rpcBlockJSON and rpcTxJSON are the few fields of the JSON-RPC block and
// transaction shapes this matrix reads, decoded straight from the wire for the
// reason given in getBlock. A pointer field distinguishes "absent" from "zero",
// which matters when the question is whether a block or transaction exists.
type rpcBlockJSON struct {
	Number       *hexutil.Uint64 `json:"number"`
	Hash         *ethcommon.Hash `json:"hash"`
	ParentHash   *ethcommon.Hash `json:"parentHash"`
	Transactions []rpcTxJSON     `json:"transactions"`
}

type rpcTxJSON struct {
	Hash             ethcommon.Hash  `json:"hash"`
	TransactionIndex *hexutil.Uint64 `json:"transactionIndex"`
}

// getBlock reads one block as a gateway's RPC reports it, nil when the gateway
// has no such block.
//
// Raw JSON rather than ethclient.BlockByNumber: ethclient rebuilds a
// types.Block, whose Hash() recomputes a keccak over the header — but our block
// hashes come from Fabric and are stored as-is, so the recomputed value is not
// a hash the gateway has ever heard of, and every hash-keyed call made with it
// would miss. A transport error is fatal; it means we learned nothing, which is
// different from learning that the block is absent.
func getBlock(t *testing.T, p nsParty, blockNum uint64) *rpcBlockJSON {
	t.Helper()

	var raw *rpcBlockJSON
	if err := p.Eth.Client().CallContext(t.Context(), &raw, "eth_getBlockByNumber",
		hexutil.EncodeUint64(blockNum), true); err != nil {
		t.Fatalf("ns=%s: eth_getBlockByNumber(%d): %v", p.Namespace, blockNum, err)
	}
	if raw == nil || raw.Hash == nil {
		return nil
	}
	return raw
}

// txByBlockAndIndex is eth_getTransactionByBlockNumberAndIndex, nil when the
// gateway reports nothing at that index. By number rather than hash, so the
// recomputed-hash problem in getBlock cannot be mistaken for an empty slot.
func txByBlockAndIndex(t *testing.T, p nsParty, blockNum, index uint64) *rpcTxJSON {
	t.Helper()

	var got *rpcTxJSON
	if err := p.Eth.Client().CallContext(t.Context(), &got, "eth_getTransactionByBlockNumberAndIndex",
		hexutil.EncodeUint64(blockNum), hexutil.EncodeUint64(index)); err != nil {
		t.Fatalf("ns=%s: eth_getTransactionByBlockNumberAndIndex(%d, %d): %v",
			p.Namespace, blockNum, index, err)
	}
	return got
}

// heightTimeout bounds how long the gateways are given to agree on a height.
// The party that committed has primer.WaitForCommit as its synchronization
// point; the idle party has none, so the only way to say anything about its
// view of a block it has nothing in is to wait for it.
const heightTimeout = 10 * time.Second

// assertSameHeight waits for every party to report the same eth_blockNumber, at
// least atLeast.
//
// Agreement is the claim: block numbers are the channel's, not the namespace's,
// so a gateway with nothing of its own in the last few blocks still counts
// them. A namespace-local sequence would show up as the idle gateway
// permanently behind.
func assertSameHeight(t *testing.T, parties []nsParty, atLeast uint64) {
	t.Helper()

	deadline := time.Now().Add(heightTimeout)
	for {
		heights := make([]uint64, len(parties))
		for i, p := range parties {
			h, err := p.Eth.BlockNumber(t.Context())
			if err != nil {
				t.Fatalf("ns=%s: eth_blockNumber: %v", p.Namespace, err)
			}
			heights[i] = h
		}

		agreed := heights[0] >= atLeast
		for _, h := range heights[1:] {
			if h != heights[0] {
				agreed = false
			}
		}
		if agreed {
			return
		}

		if time.Now().After(deadline) {
			t.Fatalf("gateways never agreed on a height >= %d within %s; last reading %v. "+
				"A gateway stuck below the others means blocks that are empty after "+
				"namespace filtering never reach it.", atLeast, heightTimeout, heights)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// assertSameBlock checks that two gateways describe the same Fabric block under
// one number. Without this, everything asserted about the contents of "block n"
// would only say that each gateway has some block with that number.
func assertSameBlock(t *testing.T, blockNum uint64, a nsParty, ablk *rpcBlockJSON, b nsParty, bblk *rpcBlockJSON) {
	t.Helper()

	if *ablk.Hash != *bblk.Hash {
		t.Errorf("block %d: ns=%s reports hash %s, ns=%s reports %s",
			blockNum, a.Namespace, ablk.Hash, b.Namespace, bblk.Hash)
	}
	if parentOf(ablk) != parentOf(bblk) {
		t.Errorf("block %d: ns=%s reports parentHash %s, ns=%s reports %s",
			blockNum, a.Namespace, parentOf(ablk), b.Namespace, parentOf(bblk))
	}
}

// parentOf is a block's parentHash, zero when absent.
func parentOf(blk *rpcBlockJSON) ethcommon.Hash {
	if blk.ParentHash == nil {
		return ethcommon.Hash{}
	}
	return *blk.ParentHash
}

// assertIndexRoundTrip checks the by-index path into a block: every transaction
// the gateway lists comes back at the transactionIndex it reports for itself,
// and no index up to probeUpTo yields a transaction the gateway does not list.
//
// The probe matters more than the round trip. In a shared block some indices
// below probeUpTo are occupied by the other namespace's transactions in the
// Fabric block, so this is the leakage question asked through the index path
// rather than by hash.
func assertIndexRoundTrip(t *testing.T, p nsParty, blockNum uint64, blk *rpcBlockJSON, probeUpTo uint64) {
	t.Helper()

	listed := make(map[ethcommon.Hash]bool, len(blk.Transactions))
	for _, tx := range blk.Transactions {
		listed[tx.Hash] = true
	}

	for _, tx := range blk.Transactions {
		if tx.TransactionIndex == nil {
			t.Errorf("ns=%s: block %d lists tx %s with no transactionIndex", p.Namespace, blockNum, tx.Hash)
			continue
		}
		idx := uint64(*tx.TransactionIndex)
		if idx > probeUpTo {
			probeUpTo = idx
		}

		switch got := txByBlockAndIndex(t, p, blockNum, idx); {
		case got == nil:
			t.Errorf("ns=%s: block %d lists tx %s at transactionIndex %#x, but nothing is returned there",
				p.Namespace, blockNum, tx.Hash, idx)
		case got.Hash != tx.Hash:
			t.Errorf("ns=%s: block %d index %#x returns tx %s, want %s",
				p.Namespace, blockNum, idx, got.Hash, tx.Hash)
		}
	}

	for i := uint64(0); i <= probeUpTo; i++ {
		if got := txByBlockAndIndex(t, p, blockNum, i); got != nil && !listed[got.Hash] {
			t.Errorf("LEAK: gateway for ns=%s returns tx %s at block %d index %d, which it does not list in that block",
				p.Namespace, got.Hash, blockNum, i)
		}
	}
}

// assertSharedBlock checks both gateways' view of a block that carried one
// transaction from each namespace; txs is indexed by party, as commitConcurrently
// returns it.
//
// This is the sharpest form of the isolation question: one block number, one
// range of ledger positions, two namespaces. Both gateways must agree on which
// Fabric block it is and disagree on its contents in exactly the way the
// namespace filter prescribes.
func assertSharedBlock(t *testing.T, parties []nsParty, blockNum uint64, txs []*types.Transaction) {
	t.Helper()

	blocks := make([]*rpcBlockJSON, len(parties))
	probeUpTo := uint64(0)
	for i, p := range parties {
		blk := getBlock(t, p, blockNum)
		if blk == nil {
			t.Fatalf("ns=%s: block %d is missing, but this namespace committed in it", p.Namespace, blockNum)
		}
		blocks[i] = blk
		for _, tx := range blk.Transactions {
			if tx.TransactionIndex != nil && uint64(*tx.TransactionIndex) > probeUpTo {
				probeUpTo = uint64(*tx.TransactionIndex)
			}
		}
	}
	for i := 1; i < len(blocks); i++ {
		assertSameBlock(t, blockNum, parties[0], blocks[0], parties[i], blocks[i])
	}

	for i, p := range parties {
		listed := make(map[ethcommon.Hash]bool, len(blocks[i].Transactions))
		for _, tx := range blocks[i].Transactions {
			listed[tx.Hash] = true
		}

		if !listed[txs[i].Hash()] {
			t.Errorf("ns=%s: block %d does not list this namespace's own tx %s",
				p.Namespace, blockNum, txs[i].Hash())
		}
		for j := range parties {
			if j != i && listed[txs[j].Hash()] {
				t.Errorf("LEAK: gateway for ns=%s lists tx %s (ns=%s) in its view of block %d",
					p.Namespace, txs[j].Hash(), parties[j].Namespace, blockNum)
			}
		}
		// probeUpTo spans both parties' positions, so each gateway is probed
		// across the other's too.
		assertIndexRoundTrip(t, p, blockNum, blocks[i], probeUpTo)
	}
}

// assertEmptyBlockPublished checks that a block one namespace committed in is
// still published to the other, carrying nothing.
//
// This is the "blocks are forwarded even when empty" invariant from
// docs/ARCHITECTURE.md seen from the RPC surface. The stores take their resume
// height from the blocks they are handed, so a block whose transactions all
// belong to another namespace has to arrive empty rather than not arrive: a gap
// in the sequence would be indistinguishable downstream from a block that went
// missing.
func assertEmptyBlockPublished(t *testing.T, idle, active nsParty, blockNum uint64) {
	t.Helper()

	committed := getBlock(t, active, blockNum)
	if committed == nil {
		t.Fatalf("ns=%s: block %d is missing on the gateway that committed in it", active.Namespace, blockNum)
	}

	blk := getBlock(t, idle, blockNum)
	if blk == nil {
		t.Errorf("ns=%s: block %d is missing, but ns=%s committed in it: a block carrying "+
			"nothing for a namespace must still be published to it, empty",
			idle.Namespace, blockNum, active.Namespace)
		return
	}

	assertSameBlock(t, blockNum, active, committed, idle, blk)

	if n := len(blk.Transactions); n != 0 {
		t.Errorf("ns=%s: block %d lists %d transaction(s), want none: only ns=%s committed in it",
			idle.Namespace, blockNum, n, active.Namespace)
	}
	switch logs, err := blockLogs(t.Context(), idle, blockNum); {
	case err != nil:
		t.Fatalf("ns=%s: FilterLogs for block %d: %v", idle.Namespace, blockNum, err)
	case len(logs) != 0:
		t.Errorf("ns=%s: block %d reports %d log(s), want none: only ns=%s committed in it",
			idle.Namespace, blockNum, len(logs), active.Namespace)
	}
}

// newParty deploys this namespace's contract and returns the party that owns
// it. Each party gets a fresh EthClient, so its key, contract address and
// transaction hashes are its own and a leak is unambiguous.
//
// The contract is GLDToken because it emits an indexed Transfer event, which is
// what gives eth_getLogs something namespace-specific to leak.
func newParty(t *testing.T, namespace string, gw *core.Gateway, chainConfig *params.ChainConfig, supply *big.Int) nsParty {
	t.Helper()

	client, err := NewEthClient(contracts.GLDTokenMetaData, chainConfig)
	if err != nil {
		t.Fatalf("ns=%s: eth client: %v", namespace, err)
	}

	eth, err := NewNativeEthClient(gw)
	if err != nil {
		t.Fatalf("ns=%s: native eth client: %v", namespace, err)
	}

	addr := deploySmartContract(t, gw, client, supply)

	return nsParty{
		Namespace: namespace,
		Gateway:   gw,
		Eth:       eth,
		Contract:  addr,
		Client:    client,
	}
}

// blockLogs returns every log a gateway reports for one block.
func blockLogs(ctx context.Context, p nsParty, blockNum uint64) ([]types.Log, error) {
	n := new(big.Int).SetUint64(blockNum)
	return p.Eth.FilterLogs(ctx, ethereum.FilterQuery{FromBlock: n, ToBlock: n})
}

// addressLogs returns every log a gateway reports for one contract address,
// across the whole chain. Used to check that a contract deployed on one
// namespace never emits logs on the other's gateway.
func addressLogs(ctx context.Context, p nsParty, addr ethcommon.Address) ([]types.Log, error) {
	return p.Eth.FilterLogs(ctx, ethereum.FilterQuery{
		FromBlock: big.NewInt(0),
		Addresses: []ethcommon.Address{addr},
	})
}

// assertNoLeakage checks that a gateway surfaces none of another namespace's
// transactions, receipts or logs, asked by hash and by contract address rather
// than per block.
//
// Each party deploys its own contract from its own key, so any appearance of
// the other party's tx hash or contract address is unambiguous leakage.
func assertNoLeakage(t *testing.T, self, other nsParty, otherTxs []ethcommon.Hash) {
	t.Helper()
	ctx := t.Context()

	for _, h := range otherTxs {
		if tx, _, err := self.Eth.TransactionByHash(ctx, h); err == nil && tx != nil {
			t.Errorf("LEAK: gateway for ns=%s returned tx %s belonging to ns=%s",
				self.Namespace, h, other.Namespace)
		}
		if r, err := self.Eth.TransactionReceipt(ctx, h); err == nil && r != nil {
			t.Errorf("LEAK: gateway for ns=%s returned a receipt for tx %s belonging to ns=%s",
				self.Namespace, h, other.Namespace)
		}
	}

	// Logs are filtered by the other party's contract address, so a hit here
	// is that contract's event surfacing on the wrong gateway.
	//
	// An error is fatal rather than logged: every harness registers a FilterAPI
	// for its gateway (defaultHandlerChain), so a failure here is a broken
	// filter path, not a backend that does not serve logs — and carrying on
	// would leave log leakage unchecked while the test still passed.
	logs, err := addressLogs(ctx, self, other.Contract)
	if err != nil {
		t.Fatalf("ns=%s: FilterLogs for foreign address %s: %v", self.Namespace, other.Contract, err)
	}
	if len(logs) > 0 {
		t.Errorf("LEAK: gateway for ns=%s returned %d log(s) from contract %s deployed on ns=%s",
			self.Namespace, len(logs), other.Contract, other.Namespace)
	}
}

// assertOwnTxsVisible checks that each party still sees everything it
// committed. Without it, "no leakage" would be trivially satisfiable by a
// gateway that returns nothing at all.
func assertOwnTxsVisible(t *testing.T, parties []nsParty, ownTxs [][]ethcommon.Hash) {
	t.Helper()
	ctx := t.Context()

	for i, p := range parties {
		for _, h := range ownTxs[i] {
			if tx, _, err := p.Eth.TransactionByHash(ctx, h); err != nil || tx == nil {
				t.Errorf("ns=%s lost its own tx %s: %v", p.Namespace, h, err)
			}
		}
	}
}

// commitConcurrently submits one transaction per party and only then waits for
// both, so the orderer can batch them into a single block. The existing
// callSmartContract helper submits and waits before returning, which serializes
// and guarantees one transaction per block — the opposite of what the shared-block
// case needs.
//
// Returns the submitted transactions in the order the parties were given.
func commitConcurrently(t *testing.T, parties []nsParty, method string, args ...any) []*types.Transaction {
	t.Helper()
	ctx := t.Context()

	txs := make([]*types.Transaction, len(parties))
	for i, p := range parties {
		tx, err := p.Client.TxForCall(ctx, p.Gateway, &p.Contract, method, args...)
		if err != nil {
			t.Fatalf("ns=%s: build %s: %v", p.Namespace, method, err)
		}
		txs[i] = tx
	}

	// Submit everything before waiting for anything.
	for i, p := range parties {
		if err := p.Eth.SendTransaction(ctx, txs[i]); err != nil {
			t.Fatalf("ns=%s: send %s: %v", p.Namespace, method, err)
		}
	}

	for i, p := range parties {
		if err := primer.WaitForCommit(ctx, p.Eth, txs[i]); err != nil {
			t.Fatalf("ns=%s: wait for %s: %v", p.Namespace, method, err)
		}
	}

	return txs
}

// blockOf reports the block a committed transaction landed in, per the gateway
// that owns it.
func blockOf(t *testing.T, p nsParty, tx *types.Transaction) uint64 {
	t.Helper()
	r, err := p.Eth.TransactionReceipt(t.Context(), tx.Hash())
	if err != nil {
		t.Fatalf("ns=%s: receipt for %s: %v", p.Namespace, tx.Hash(), err)
	}
	return r.BlockNumber.Uint64()
}

// sharedBlockOf says whether the parties' transactions landed in one block, and
// which blocks they landed in. An orderer that cut a block per transaction
// would otherwise leave the test silently checking the single-namespace case
// twice.
func sharedBlockOf(t *testing.T, parties []nsParty, txs []*types.Transaction) (shared bool, blocks []uint64) {
	t.Helper()
	blocks = make([]uint64, len(parties))
	for i := range parties {
		blocks[i] = blockOf(t, parties[i], txs[i])
	}
	for i := 1; i < len(blocks); i++ {
		if blocks[i] == blocks[0] {
			shared = true
		}
	}
	return shared, blocks
}

// mixedBlockAttempts is how many times the matrix tries to land both
// namespaces' transactions in one block before giving up.
//
// Retried because batching is timing-dependent, not because a shared block is
// optional: an orderer cuts on whichever of its size and time thresholds trips
// first, so a batch boundary can fall between two concurrent submissions
// however promptly they are sent. A backend that batches at all shares a block
// within a few attempts; one that cuts a block per transaction never will, and
// that is the case worth failing on.
const mixedBlockAttempts = 5

// commitMixedBlock commits one transfer per party, repeating until both land in
// the same block, and returns that block along with every transaction it
// committed (grouped per attempt, the successful one last).
//
// Not achieving a shared block is fatal: it is the case this matrix exists to
// check, so a run that never gets one has not tested what it set out to test
// and must not report success.
func commitMixedBlock(t *testing.T, backend string, parties []nsParty, recipient ethcommon.Address) (uint64, [][]*types.Transaction) {
	t.Helper()

	var committed [][]*types.Transaction
	for attempt := 1; attempt <= mixedBlockAttempts; attempt++ {
		txs := commitConcurrently(t, parties, "transfer", recipient, big.NewInt(1))
		committed = append(committed, txs)

		shared, blocks := sharedBlockOf(t, parties, txs)
		if shared {
			t.Logf("shared-block case: both namespaces landed in block %d (attempt %d/%d)",
				blocks[0], attempt, mixedBlockAttempts)
			return blocks[0], committed
		}
		t.Logf("shared-block attempt %d/%d: landed in separate blocks %v, retrying",
			attempt, mixedBlockAttempts, blocks)
	}

	t.Fatalf("backend=%s never produced a block carrying both namespaces in %d attempts.\n"+
		"Most likely the orderer is cutting one transaction per block — on the fabric-x "+
		"test committer that is SC_ORDERER_BLOCK_SIZE in compose.fabric-x.yml, which "+
		"start-x expects to be 2. Note that the value in testdata/config/mock-orderer.yaml "+
		"would be ignored: the environment variable outranks it.", backend, mixedBlockAttempts)
	return 0, nil // unreachable: Fatalf stops the test
}

// runNamespaceIsolationMatrix is the shared body every backend runs. parties
// must be two gateways on the same channel, each configured with its own
// namespace and each already serving RPC.
//
// It drives two situations — a block carrying both namespaces, and a block
// carrying one of them while the other is idle, in each direction — and asserts
// the properties listed at the top of this file against both.
func runNamespaceIsolationMatrix(t *testing.T, backend string, parties []nsParty) {
	t.Helper()
	if len(parties) != 2 {
		t.Fatalf("namespace matrix needs exactly 2 parties, got %d", len(parties))
	}

	t.Logf("backend=%s namespaces=[%s, %s]", backend, parties[0].Namespace, parties[1].Namespace)

	// Each party's contract is deployed and holds its own supply, so a
	// transfer emits a Transfer event on that namespace only.
	ownTxs := make([][]ethcommon.Hash, len(parties))

	// ── A block carrying both namespaces ──────────────────────────────────
	recipient := ethcommon.HexToAddress("0x000000000000000000000000000000000000c0de")
	sharedBlock, attempts := commitMixedBlock(t, backend, parties, recipient)
	for _, txs := range attempts {
		for i := range parties {
			ownTxs[i] = append(ownTxs[i], txs[i].Hash())
		}
	}

	assertSameHeight(t, parties, sharedBlock)
	assertSharedBlock(t, parties, sharedBlock, attempts[len(attempts)-1])

	// ── Single-namespace blocks, in both directions ───────────────────────
	// Each party in turn commits alone while the other stays idle, so the
	// empty-after-filter case is exercised from both sides. One direction
	// alone would miss a filter that works for one namespace but not the
	// other — for instance one sensitive to the order namespaces appear in a
	// block, or to which party's gateway happens to be first in the harness.
	for i := range parties {
		actor, idle := parties[i], parties[1-i]

		solo := commitConcurrently(t, parties[i:i+1], "transfer", recipient, big.NewInt(2))
		ownTxs[i] = append(ownTxs[i], solo[0].Hash())
		soloBlock := blockOf(t, actor, solo[0])
		t.Logf("single-namespace case: ns=%s committed alone in block %d (ns=%s idle)",
			actor.Namespace, soloBlock, idle.Namespace)

		assertSameHeight(t, parties, soloBlock)
		assertEmptyBlockPublished(t, idle, actor, soloBlock)
	}

	// ── Leakage, across everything committed above ────────────────────────
	assertNoLeakage(t, parties[0], parties[1], ownTxs[1])
	assertNoLeakage(t, parties[1], parties[0], ownTxs[0])
	assertOwnTxsVisible(t, parties, ownTxs)
}
