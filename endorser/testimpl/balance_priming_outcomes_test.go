/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package testimpl

import (
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/hyperledger/fabric-x-evm/api/endorsementpb"
	fxcommon "github.com/hyperledger/fabric-x-evm/common"
	"github.com/hyperledger/fabric-x-evm/endorser/storage"
)

// A revert or an exec failure is a committed outcome with a read-write set, not an error, as in execution.Endorse.
func TestBalancePrimingExecutor_FailedExecutionIsAnOutcome(t *testing.T) {
	for _, tc := range []struct {
		name     string
		initCode []byte
		want     endorsementpb.ExecutionStatus
	}{
		{"revert", []byte{0x60, 0x00, 0x60, 0x00, 0xfd}, endorsementpb.ExecutionStatus_EXECUTION_STATUS_REVERTED},
		{"invalid opcode", []byte{0xfe}, endorsementpb.ExecutionStatus_EXECUTION_STATUS_EXEC_FAILED},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ex, err := NewBalancePrimingExecutor(t.Context(), "testns", storage.NewLightKVS(8), testEVMConfig(), true, nil, nil)
			if err != nil {
				t.Fatalf("NewBalancePrimingExecutor: %v", err)
			}
			t.Cleanup(func() { ex.Close() })

			key, err := crypto.GenerateKey()
			if err != nil {
				t.Fatal(err)
			}
			tx, err := types.SignTx(types.NewContractCreation(0, big.NewInt(0), 100_000, big.NewInt(0), tc.initCode),
				types.LatestSignerForChainID(big.NewInt(4011)), key)
			if err != nil {
				t.Fatal(err)
			}

			res, err := ex.Execute(tx)
			if err != nil {
				t.Fatalf("Execute returned an error for a committed outcome: %v", err)
			}
			md, err := fxcommon.UnmarshalExecutionMetadata(res.Payload)
			if err != nil {
				t.Fatalf("payload: %v", err)
			}
			if md.GetStatus() != tc.want {
				t.Errorf("status = %v, want %v", md.GetStatus(), tc.want)
			}
			if len(res.RWS.Writes) == 0 {
				t.Error("a committed outcome must carry a read-write set (the nonce write)")
			}
		})
	}
}
