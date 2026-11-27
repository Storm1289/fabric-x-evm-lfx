/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: LGPL-3.0-or-later
*/

package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	eapp "github.com/hyperledger/fabric-x-evm/endorser/app"
	"github.com/hyperledger/fabric-x-evm/gateway/app"
	"github.com/hyperledger/fabric-x-evm/gateway/config"
	"github.com/spf13/cobra"
	"google.golang.org/grpc/grpclog"
)

func main() {
	// silence GRPC logging
	grpclog.SetLoggerV2(grpclog.NewLoggerV2(io.Discard, os.Stderr, os.Stderr))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	root := &cobra.Command{
		Use:   "fxevm",
		Short: "fxevm - Fabric-X EVM gateway and endorser",
	}

	root.AddCommand(newStartCmd())
	root.AddCommand(newTestNodeCmd())
	root.AddCommand(newHealthcheckCmd())

	if err := root.ExecuteContext(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

// newStartCmd starts a process described by the given config: a gateway with
// its embedded endorser if gateway: is present, a standalone endorser
// otherwise.
func newStartCmd() *cobra.Command {
	var configPath string

	cmd := &cobra.Command{
		Use:   "start",
		Short: "Start a gateway (with embedded endorser) or a standalone endorser, config-driven",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runStart(cmd.Context(), configPath)
		},
	}

	cmd.Flags().StringVarP(&configPath, "config", "c", "", "Path to the YAML configuration file")
	_ = cmd.MarkFlagRequired("config")
	return cmd
}

func runStart(ctx context.Context, configPath string) error {
	cfg, err := config.Load(configPath)
	if err != nil {
		return err
	}
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("invalid config %q: %w", configPath, err)
	}
	flogging.Init(flogging.Config{
		Format:  cfg.Logging.Format,
		LogSpec: cfg.Logging.Spec,
	})

	if cfg.Gateway == nil {
		application, err := eapp.New(ctx, cfg)
		if err != nil {
			return err
		}
		return application.Run(ctx)
	}

	application, err := app.New(ctx, cfg)
	if err != nil {
		return err
	}

	return application.Run(ctx)
}

// newTestNodeCmd returns the command to start a test node with test RPC enabled.
//
// It is fully self-contained: an in-process fabrictest network, ephemeral
// identities, embedded test accounts — no file, no docker/Fablo backend. The
// test RPC needs that in-process network: against a real backend evm_revert
// would make the endorser disagree with the committer, evm_mine can't cut
// blocks, and directly funded accounts never reach the committer.
func newTestNodeCmd() *cobra.Command {
	var testAccountsPath string
	var listen string
	var chainID int64
	var protocol string

	cmd := &cobra.Command{
		Use:   "testnode",
		Short: "Start a standalone test node with test RPC enabled",
		Long: `Start a self-contained node for local development and testing with
Hardhat, Foundry, viem and similar tools.

The node runs its own in-process network with in-memory storage. It comes with
funded accounts that the node signs for (eth_accounts, eth_sendTransaction), and
supports the hardhat_* and evm_* methods for setting state, mining blocks, and
snapshots and reverts. Transactions return once they are in a block.

Because the node holds the account keys, don't expose it beyond your machine.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runTestNode(cmd.Context(), listen, chainID, protocol, testAccountsPath)
		},
	}

	cmd.Flags().StringVar(&testAccountsPath, "test-accounts-path", "", "Path to JSON file containing test accounts with private keys (default: embedded Hardhat accounts)")
	cmd.Flags().StringVar(&listen, "listen", ":8545", "HTTP listen address")
	cmd.Flags().Int64Var(&chainID, "chain-id", 31337, "Ethereum chain ID")
	cmd.Flags().StringVar(&protocol, "protocol", "fabric-x", "Network protocol: fabric or fabric-x")

	return cmd
}

// runTestNode starts a testnode with no external dependencies: an in-process
// fabrictest network, ephemeral local identities, in-memory storage.
func runTestNode(ctx context.Context, listen string, chainID int64, protocol, testAccountsPath string) error {
	application, err := app.NewTestNode(ctx, app.TestNodeConfig{
		Listen:           listen,
		ChainID:          chainID,
		Protocol:         protocol,
		TestAccountsPath: testAccountsPath,
	})
	if err != nil {
		return err
	}
	return application.Run(ctx)
}
