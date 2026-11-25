/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package execution

import (
	"context"

	"github.com/hyperledger/fabric-x-sdk/blocks"
	"github.com/hyperledger/fabric-x-sdk/state"
)

// testVersionedDBSnapshotter is a state.Snapshotter over a *state.VersionedDB whose
// readers expose the height they read at, so tests can check which height the
// engine asked for.
type testVersionedDBSnapshotter struct {
	db *state.VersionedDB
}

func (w *testVersionedDBSnapshotter) Snapshot(ctx context.Context) (ReadStore, error) {
	latest, err := w.db.BlockNumber(ctx)
	if err != nil {
		return nil, err
	}
	return w.SnapshotAt(ctx, latest)
}

func (w *testVersionedDBSnapshotter) SnapshotAt(_ context.Context, block uint64) (ReadStore, error) {
	return &testVersionedDBReader{db: w.db, blockNumber: block}, nil
}

type testVersionedDBReader struct {
	db          *state.VersionedDB
	blockNumber uint64
}

func (r *testVersionedDBReader) Get(namespace, key string) (*blocks.WriteRecord, error) {
	return r.db.GetAt(namespace, key, r.blockNumber)
}

func (r *testVersionedDBReader) Close() error { return nil }
