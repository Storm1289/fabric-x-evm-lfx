/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package core

import (
	"context"
	"testing"

	"github.com/hyperledger/fabric-x-sdk/blocks"
	"github.com/stretchr/testify/require"
)

func TestNew_InitializesNonceGate(t *testing.T) {
	g, err := New(nil, nil, nil, testChainID, 1, nil, nil)
	require.NoError(t, err)
	require.NotNil(t, g.nonceGate)
	require.NotNil(t, g.TxQueue)
}

func gatewayWithGate(t *testing.T) *Gateway {
	t.Helper()
	cfg, signer := chainCtx(t)
	g := &Gateway{
		ChainConfig: cfg,
		Signer:      signer,
		TxQueue:     NewTxQueue(),
		endorsers:   newClient(nonceStub()),
	}
	g.nonceGate = newNonceGate(g, g.Signer, g.TxQueue.Enqueue)
	return g
}

func TestSendTransaction_FutureNonceParked(t *testing.T) {
	key := newKey(t)
	g := gatewayWithGate(t)

	// Committed nonce is 0; a nonce-3 tx must be parked, not enqueued.
	tx := newValidTx(t, key, validTxOpts{nonce: 3})
	require.NoError(t, g.SendTransaction(context.Background(), tx))

	// Not in the worker queue...
	require.Nil(t, g.TxQueue.IsPending(tx.Hash()))

	// ...but reported as pending (parked) via TransactionByHash.
	dtx, err := g.TransactionByHash(context.Background(), tx.Hash())
	require.NoError(t, err)
	require.NotNil(t, dtx)
	require.Equal(t, uint64(0), dtx.BlockNumber) // 0 signals pending to the API layer
}

func TestHandle_EmptyBlockReleasesNothing(t *testing.T) {
	g := gatewayWithGate(t)
	require.NoError(t, g.Handle(context.Background(), blocks.Block{}))
}
