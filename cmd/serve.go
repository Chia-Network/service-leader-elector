package cmd

import (
	"context"
	"os/signal"
	"syscall"

	"github.com/chia-network/go-modules/pkg/slogs"
	"github.com/chia-network/service-leader-elector/internal/election"
	"github.com/chia-network/service-leader-elector/internal/server"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// serveCmd represents the serve command
var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Runs leader election and the status HTTP server",
	Run: func(cmd *cobra.Command, args []string) {
		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()

		serverErr := make(chan error, 1)
		go func() {
			err := server.Run(ctx, viper.GetInt("port"))
			if err != nil {
				// Cancel election first so leader label / lease cleanup can run
				// before the process exits.
				slogs.Logr.Error("status server failed, shutting down", "error", err)
				stop()
			}
			serverErr <- err
		}()

		election.Run(ctx, viper.GetString("lease-name"))

		if err := <-serverErr; err != nil {
			slogs.Logr.Fatal("status server", "error", err)
		}
	},
}

func init() {
	rootCmd.AddCommand(serveCmd)

	serveCmd.Flags().String("lease-name", "service-leader-elector", "Name of the Lease to contend for in the current namespace")
	serveCmd.Flags().Int("port", 8080, "HTTP port for the status server")

	cobra.CheckErr(viper.BindPFlag("lease-name", serveCmd.Flags().Lookup("lease-name")))
	cobra.CheckErr(viper.BindPFlag("port", serveCmd.Flags().Lookup("port")))
}
