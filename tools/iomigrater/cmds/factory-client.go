package cmd

import (
	"github.com/spf13/cobra"

	"github.com/iotexproject/iotex-core/tools/iomigrater/common"
)

var (
	// StateDB2Factory Used to Sub command.
	FactoryClient = &cobra.Command{
		Use:   "",
		Short: "",
		Long:  "",
		RunE: func(cmd *cobra.Command, args []string) error {
			return factoryClient()
		},
	}
)

var (
	getKV = false
	putKV = false
)

func init() {
	FactoryClient.PersistentFlags().StringVarP(&factoryFile, "factory", "f", "", common.TranslateInLang(stateDB2FactoryFlagFactoryFileUse))
}

func factoryClient() error {

	return nil
}
