package main

import (
	"fmt"
	"os"

	"github.com/memoio/pay-gateway/cmd"
	"github.com/urfave/cli/v2"
)

func main() {
	local := make([]*cli.Command, 0)
	local = append(local, cmd.GatewayRunCmd)
	local = append(local, cmd.GatewayStopCmd)

	app := &cli.App{
		Name:     "gateway",
		Usage:    "memo pay gateway",
		Commands: local,
	}

	app.Setup()

	err := app.Run(os.Args)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %s\n\n", err)
		os.Exit(1)
	}
}
