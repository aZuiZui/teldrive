package cmd

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/spf13/cobra"
	"github.com/tgdrive/teldrive/pkg/services"
)

// NewRefusedCmd prints the parts the running teldrive saw Telegram refuse in the last hour (JSON,
// from its own memory: reader.RefusedParts). Run it in teldrive's container: the server answers
// that list only to requests from the same machine.
func NewRefusedCmd() *cobra.Command {
	var port int
	cmd := &cobra.Command{
		Use:   "refused",
		Short: "List the parts Telegram refused recently, as the running teldrive saw them (JSON)",
		Example: `  docker exec teldrive /teldrive refused
  teldrive refused --port 5051`,
		RunE: func(cmd *cobra.Command, args []string) error {
			client := http.Client{Timeout: 20 * time.Second}
			resp, err := client.Get(fmt.Sprintf("http://127.0.0.1:%d%s", port, services.RefusedPath))
			if err != nil {
				return fmt.Errorf("teldrive not answering on port %d: %w", port, err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				return fmt.Errorf("teldrive on port %d: %s (older than 1.8.3-nasfix8?)", port, resp.Status)
			}
			_, err = io.Copy(os.Stdout, resp.Body)
			return err
		},
	}
	cmd.Flags().IntVar(&port, "port", 8080, "The teldrive server's port (server.port)")
	return cmd
}
