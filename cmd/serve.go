package cmd

import (
	"context"
	"os/signal"
	"syscall"

	"github.com/chia-network/service-leader-elector/internal/election"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// serveCmd represents the serve command
var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Runs the readiness probe server",
	Run: func(cmd *cobra.Command, args []string) {
		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()

		election.Run(ctx, viper.GetString("lease-name"))
	},
}

func init() {
	rootCmd.AddCommand(serveCmd)

	serveCmd.Flags().String("lease-name", "service-leader-elector", "Name of the Lease to contend for in the current namespace")
	cobra.CheckErr(viper.BindPFlag("lease-name", serveCmd.Flags().Lookup("lease-name")))
}
