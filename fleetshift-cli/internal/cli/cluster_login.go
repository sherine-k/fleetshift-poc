package cli

import (
	"fmt"
	"os"
	"path/filepath"

	pb "github.com/fleetshift/fleetshift-poc/fleetshift-server/gen/fleetshift/v1"
	"github.com/spf13/cobra"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

func newClusterLoginCmd(ctx *cmdContext) *cobra.Command {
	var kubeconfigPath string
	setCurrentContext := true

	cmd := &cobra.Command{
		Use:   "login <resource-id>",
		Short: "Log into a provisioned cluster",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			resourceID := args[0]
			contextName := "fleetshift/" + resourceID

			client := pb.NewClusterServiceClient(ctx.conn)
			resp, err := client.GetClusterConnectionInfo(cmd.Context(), &pb.GetClusterConnectionInfoRequest{
				ResourceId: resourceID,
			})
			if err != nil {
				return fmt.Errorf("failed to get cluster info: %w", err)
			}

			execArgs := buildExecArgs(cmd, resourceID)

			cluster := &clientcmdapi.Cluster{
				Server: resp.GetEndpoint(),
			}
			if caCert := resp.GetCaCert(); caCert != "" {
				cluster.CertificateAuthorityData = []byte(caCert)
			}

			authInfo := &clientcmdapi.AuthInfo{
				Exec: &clientcmdapi.ExecConfig{
					APIVersion:      "client.authentication.k8s.io/v1",
					Command:         "fleetctl",
					Args:            execArgs,
					InteractiveMode: clientcmdapi.NeverExecInteractiveMode,
				},
			}

			newContext := &clientcmdapi.Context{
				Cluster:  contextName,
				AuthInfo: contextName,
			}

			kcPath := resolveKubeconfigPath(kubeconfigPath)

			if err := mergeIntoKubeconfig(kcPath, contextName, cluster, authInfo, newContext, setCurrentContext); err != nil {
				return err
			}

			fmt.Fprintf(cmd.OutOrStdout(),
				"Logged into cluster %q. Context %q set as current context.\n",
				resourceID, contextName)
			return nil
		},
	}

	cmd.Flags().StringVar(&kubeconfigPath, "kubeconfig", "", "path to kubeconfig file (default: $KUBECONFIG or ~/.kube/config)")
	cmd.Flags().BoolVar(&setCurrentContext, "set-current-context", true, "set as current context")

	return cmd
}

func buildExecArgs(cmd *cobra.Command, resourceID string) []string {
	connectionFlags := []string{"server", "server-tls", "server-ca-file", "server-insecure"}

	execArgs := []string{"cluster", "token", resourceID}
	root := cmd.Root()
	for _, name := range connectionFlags {
		f := root.PersistentFlags().Lookup(name)
		if f != nil && f.Changed {
			if f.Value.Type() == "bool" {
				execArgs = append(execArgs, "--"+f.Name)
			} else {
				execArgs = append(execArgs, "--"+f.Name, f.Value.String())
			}
		}
	}
	return execArgs
}

func resolveKubeconfigPath(explicit string) string {
	if explicit != "" {
		return explicit
	}
	if env := os.Getenv("KUBECONFIG"); env != "" {
		return env
	}
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".kube", "config")
}

func mergeIntoKubeconfig(path, contextName string, cluster *clientcmdapi.Cluster, authInfo *clientcmdapi.AuthInfo, ctx *clientcmdapi.Context, setCurrent bool) error {
	existing, err := clientcmd.LoadFromFile(path)
	if err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("cannot read kubeconfig: %w", err)
		}
		existing = clientcmdapi.NewConfig()
	}

	existing.Clusters[contextName] = cluster
	existing.AuthInfos[contextName] = authInfo
	existing.Contexts[contextName] = ctx
	if setCurrent {
		existing.CurrentContext = contextName
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("cannot create kubeconfig directory: %w", err)
	}

	if err := clientcmd.WriteToFile(*existing, path); err != nil {
		return fmt.Errorf("cannot write kubeconfig: %w", err)
	}
	return nil
}
