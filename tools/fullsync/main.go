package main

import (
	"os"

	"github.com/iotexproject/iotex-core/v2/tools/fullsync/cmd"
)

func main() {
	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}
