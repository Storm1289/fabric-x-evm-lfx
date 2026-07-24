/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package testimpl

import (
	"context"
	"testing"

	"github.com/ethereum/go-ethereum/rpc"
)

func TestHardhatAPI_Mine_RPCRegistration(t *testing.T) {
	srv := rpc.NewServer()
	if err := srv.RegisterName("hardhat", NewHardhatAPI()); err != nil {
		t.Fatalf("RegisterName hardhat: %v", err)
	}

	client := rpc.DialInProc(srv)
	defer client.Close()

	// Hardhat accepts zero, one, or two optional hex quantities.
	cases := []struct {
		name   string
		params []any
	}{
		{"no params", nil},
		{"blocks only", []any{"0x100"}},
		{"blocks and interval", []any{"0x3e8", "0x3c"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var result any
			var err error
			if len(tc.params) == 0 {
				err = client.CallContext(context.Background(), &result, "hardhat_mine")
			} else {
				err = client.CallContext(context.Background(), &result, "hardhat_mine", tc.params...)
			}
			if err != nil {
				t.Fatalf("hardhat_mine: %v", err)
			}
			if result != nil {
				t.Fatalf("result = %#v, want nil", result)
			}
		})
	}
}
