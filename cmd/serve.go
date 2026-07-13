package cmd

import (
	"github.com/chia-network/go-modules/pkg/slogs"
	"github.com/spf13/cobra"
)

// serveCmd represents the serve command
var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Runs the readiness probe server",
	Run: func(cmd *cobra.Command, args []string) {
		slogs.Logr.Info("Serve Called")
	},
}

func init() {
	rootCmd.AddCommand(serveCmd)
}
