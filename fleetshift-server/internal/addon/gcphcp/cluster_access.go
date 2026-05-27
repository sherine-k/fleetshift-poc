package gcphcp

import (
	"context"
	"fmt"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

type ClusterAccessOption func(*ClusterAccess)

func WithEndpoints(sts, iam string) ClusterAccessOption {
	return func(ca *ClusterAccess) {
		ca.stsEndpoint = sts
		ca.iamEndpoint = iam
	}
}

type ClusterAccess struct {
	gateway     GatewayConfig
	stsEndpoint string
	iamEndpoint string
}

func NewClusterAccess(gateway GatewayConfig, opts ...ClusterAccessOption) *ClusterAccess {
	ca := &ClusterAccess{gateway: gateway}
	for _, o := range opts {
		o(ca)
	}
	return ca
}

func (ca *ClusterAccess) MintCredential(ctx context.Context, callerToken string, target domain.TargetInfo) (*domain.ClusterCredential, error) {
	tc := TargetConfigFromProperties(target.Properties)

	auth := NewBrokerAuth(BrokerAuthConfig{
		WorkforcePool:     tc.WorkforcePool,
		WorkforceProvider: tc.WorkforceProvider,
		GCPProject:        tc.GCPProject,
		BrokerSAEmail:     tc.BrokerSAEmail,
		GatewayAudience:   ca.gateway.Audience,
		STSEndpoint:       ca.stsEndpoint,
		IAMEndpoint:       ca.iamEndpoint,
	})

	result, err := auth.Exchange(ctx, callerToken)
	if err != nil {
		return nil, fmt.Errorf("broker auth exchange: %w", err)
	}

	return &domain.ClusterCredential{
		Token:      result.BrokerToken,
		Expiration: time.Now().Add(55 * time.Minute),
	}, nil
}
