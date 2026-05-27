package application_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/sqlite"
)

type stubClusterAccessProvider struct {
	mintFunc func(ctx context.Context, callerToken string, target domain.TargetInfo) (*domain.ClusterCredential, error)
}

func (s *stubClusterAccessProvider) MintCredential(ctx context.Context, callerToken string, target domain.TargetInfo) (*domain.ClusterCredential, error) {
	return s.mintFunc(ctx, callerToken, target)
}

func setupClusterService(t *testing.T) (*application.ClusterService, *application.TargetService, *application.ClusterAccessRegistry) {
	t.Helper()
	db := sqlite.OpenTestDB(t)
	store := &sqlite.Store{DB: db}
	targetSvc := &application.TargetService{Store: store}
	reg := application.NewClusterAccessRegistry()
	svc := &application.ClusterService{
		Targets:   targetSvc,
		Providers: reg,
	}
	return svc, targetSvc, reg
}

func registerK8sTarget(t *testing.T, svc *application.TargetService, name string, props map[string]string, provTargetID domain.TargetID) {
	t.Helper()
	if err := svc.Register(context.Background(), domain.TargetInfo{
		ID:                   domain.TargetID("k8s-" + name),
		Type:                 "kubernetes",
		Name:                 name,
		Properties:           props,
		ProvisioningTargetID: provTargetID,
	}); err != nil {
		t.Fatalf("register k8s target %q: %v", name, err)
	}
}

func registerSeededTarget(t *testing.T, svc *application.TargetService, id domain.TargetID, typ domain.TargetType, props map[string]string) {
	t.Helper()
	if err := svc.Register(context.Background(), domain.TargetInfo{
		ID:         id,
		Type:       typ,
		Name:       string(id),
		Properties: props,
	}); err != nil {
		t.Fatalf("register seeded target %q: %v", id, err)
	}
}

func TestClusterService_GetConnectionInfo_Success(t *testing.T) {
	svc, targetSvc, _ := setupClusterService(t)

	registerK8sTarget(t, targetSvc, "my-cluster", map[string]string{
		"api_server": "https://api.example.com:6443",
		"ca_cert":    "FAKE-CA-DATA",
	}, "")

	endpoint, caCert, err := svc.GetConnectionInfo(context.Background(), "my-cluster")
	if err != nil {
		t.Fatalf("GetConnectionInfo: %v", err)
	}
	if endpoint != "https://api.example.com:6443" {
		t.Errorf("endpoint = %q, want https://api.example.com:6443", endpoint)
	}
	if caCert != "FAKE-CA-DATA" {
		t.Errorf("caCert = %q, want FAKE-CA-DATA", caCert)
	}
}

func TestClusterService_GetConnectionInfo_NoCaCert(t *testing.T) {
	svc, targetSvc, _ := setupClusterService(t)

	registerK8sTarget(t, targetSvc, "no-ca", map[string]string{"api_server": "https://api.example.com:6443"}, "")

	endpoint, caCert, err := svc.GetConnectionInfo(context.Background(), "no-ca")
	if err != nil {
		t.Fatalf("GetConnectionInfo: %v", err)
	}
	if endpoint != "https://api.example.com:6443" {
		t.Errorf("endpoint = %q, want https://api.example.com:6443", endpoint)
	}
	if caCert != "" {
		t.Errorf("caCert = %q, want empty", caCert)
	}
}

func TestClusterService_GetConnectionInfo_TargetNotFound(t *testing.T) {
	svc, _, _ := setupClusterService(t)

	_, _, err := svc.GetConnectionInfo(context.Background(), "nonexistent")
	if err == nil {
		t.Fatal("expected error for missing target")
	}
	if !errors.Is(err, domain.ErrNotFound) {
		t.Errorf("error = %v, want ErrNotFound", err)
	}
}

func TestClusterService_GetConnectionInfo_MissingAPIServer(t *testing.T) {
	svc, targetSvc, _ := setupClusterService(t)

	registerK8sTarget(t, targetSvc, "no-api", map[string]string{"ca_cert": "FAKE"}, "")

	_, _, err := svc.GetConnectionInfo(context.Background(), "no-api")
	if err == nil {
		t.Fatal("expected error for missing api_server")
	}
	if !errors.Is(err, domain.ErrNotFound) {
		t.Errorf("error = %v, want ErrNotFound", err)
	}
}

func TestClusterService_GetCredential_Success(t *testing.T) {
	svc, targetSvc, reg := setupClusterService(t)

	registerSeededTarget(t, targetSvc, "seeded-gcphcp", "gcphcp", map[string]string{"gcp_project": "my-proj"})
	registerK8sTarget(t, targetSvc, "my-cluster", map[string]string{"api_server": "https://api.example.com:6443"}, "seeded-gcphcp")

	expiration := time.Now().Add(55 * time.Minute)
	reg.Register("gcphcp", &stubClusterAccessProvider{
		mintFunc: func(_ context.Context, callerToken string, target domain.TargetInfo) (*domain.ClusterCredential, error) {
			if callerToken != "my-bearer-token" {
				t.Errorf("callerToken = %q, want my-bearer-token", callerToken)
			}
			if target.ID != "seeded-gcphcp" {
				t.Errorf("target.ID = %q, want seeded-gcphcp", target.ID)
			}
			return &domain.ClusterCredential{Token: "minted-token", Expiration: expiration}, nil
		},
	})

	cred, err := svc.GetCredential(context.Background(), "my-cluster", "my-bearer-token")
	if err != nil {
		t.Fatalf("GetCredential: %v", err)
	}
	if cred.Token != "minted-token" {
		t.Errorf("Token = %q, want minted-token", cred.Token)
	}
}

func TestClusterService_GetCredential_TargetNotFound(t *testing.T) {
	svc, _, _ := setupClusterService(t)

	_, err := svc.GetCredential(context.Background(), "nonexistent", "token")
	if err == nil {
		t.Fatal("expected error for missing target")
	}
}

func TestClusterService_GetCredential_NoProvisioningTarget(t *testing.T) {
	svc, targetSvc, _ := setupClusterService(t)

	registerK8sTarget(t, targetSvc, "orphan", map[string]string{"api_server": "https://api.example.com"}, "")

	_, err := svc.GetCredential(context.Background(), "orphan", "token")
	if err == nil {
		t.Fatal("expected error when ProvisioningTargetID is empty")
	}
}

func TestClusterService_GetCredential_ProvisioningTargetNotFound(t *testing.T) {
	svc, targetSvc, _ := setupClusterService(t)

	registerK8sTarget(t, targetSvc, "dangling", map[string]string{"api_server": "https://api.example.com"}, "gone-target")

	_, err := svc.GetCredential(context.Background(), "dangling", "token")
	if err == nil {
		t.Fatal("expected error when provisioning target does not exist")
	}
}

func TestClusterService_GetCredential_NoProviderRegistered(t *testing.T) {
	svc, targetSvc, _ := setupClusterService(t)

	registerSeededTarget(t, targetSvc, "seeded-unknown", "unknown-type", nil)
	registerK8sTarget(t, targetSvc, "no-provider", map[string]string{"api_server": "https://api.example.com"}, "seeded-unknown")

	_, err := svc.GetCredential(context.Background(), "no-provider", "token")
	if err == nil {
		t.Fatal("expected error when no provider is registered for target type")
	}
}
