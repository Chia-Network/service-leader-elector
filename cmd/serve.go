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

		sig, err := election.ParseSignal(viper.GetString("on-stopped-leading-signal"))
		if err != nil {
			slogs.Logr.Fatal("invalid on-stopped-leading-signal", "error", err)
		}

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

		election.Run(ctx, election.Config{
			LeaseName:               viper.GetString("lease-name"),
			OnStoppedLeadingProcess: viper.GetString("on-stopped-leading-process"),
			OnStoppedLeadingSignal:  sig,
		})

		if err := <-serverErr; err != nil {
			slogs.Logr.Fatal("status server", "error", err)
		}
	},
}

func init() {
	rootCmd.AddCommand(serveCmd)

	serveCmd.Flags().String("lease-name", "service-leader-elector", "Name of the Lease to contend for in the current namespace")
	serveCmd.Flags().Int("port", 8080, "HTTP port for the status server")
	serveCmd.Flags().String("on-stopped-leading-process", "", "If set, signal processes whose cmdline contains this substring when stopping leading (requires shareProcessNamespace)")
	serveCmd.Flags().String("on-stopped-leading-signal", "SIGTERM", "Signal to send to matched processes when stopping leading")

	cobra.CheckErr(viper.BindPFlag("lease-name", serveCmd.Flags().Lookup("lease-name")))
	cobra.CheckErr(viper.BindPFlag("port", serveCmd.Flags().Lookup("port")))
	cobra.CheckErr(viper.BindPFlag("on-stopped-leading-process", serveCmd.Flags().Lookup("on-stopped-leading-process")))
	cobra.CheckErr(viper.BindPFlag("on-stopped-leading-signal", serveCmd.Flags().Lookup("on-stopped-leading-signal")))
}
