package memworkflow_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain/workflowenginetest"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/delivery"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/memworkflow"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/sqlite"
)

// raceSetup creates an orchestration workflow backed by a real store
// with a real delivery reporter wired through the registry's signal
// channel, and seeds a fulfillment so orchestration completes in a
// single pass.
func raceSetup(t *testing.T) (domain.OrchestrationWorkflow, domain.FulfillmentID) {
	t.Helper()

	db := sqlite.OpenTestDB(t)
	store := &sqlite.Store{DB: db}
	vault := &sqlite.VaultStore{DB: db}

	router := delivery.NewRoutingDeliveryService()
	reg := &memworkflow.Registry{}

	reporter := application.NewDeliveryReportService(store, reg)
	recordingAgent := &sqlite.RecordingDeliveryService{
		Store:    store,
		Reporter: reporter,
		Now:      func() time.Time { return time.Date(2026, 2, 27, 12, 0, 0, 0, time.UTC) },
	}
	router.Register(workflowenginetest.TestTargetType, recordingAgent)

	orchSpec := &domain.OrchestrationWorkflowSpec{
		Store:           store,
		Delivery:        router,
		Strategies:      domain.StrategyFactory{Store: store},
		CleanupSignaler: reg,
		Vault:           vault,
	}
	orchWf, err := reg.RegisterOrchestration(orchSpec)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	fID := domain.FulfillmentID(uuid.New().String())

	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	if err := tx.Targets().Create(ctx, domain.TargetInfo{
		ID:   "t1",
		Type: workflowenginetest.TestTargetType,
		Name: "cluster-t1",
	}); err != nil {
		t.Fatal(err)
	}

	f := domain.Fulfillment{
		ID:        fID,
		State:     domain.FulfillmentStateCreating,
		CreatedAt: now,
		UpdatedAt: now,
	}
	f.AdvanceManifestStrategy(domain.ManifestStrategySpec{
		Type:      domain.ManifestStrategyInline,
		Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
	}, now)
	f.AdvancePlacementStrategy(domain.PlacementStrategySpec{
		Type:    domain.PlacementStrategyStatic,
		Targets: []domain.TargetID{"t1"},
	}, now)
	f.AdvanceRolloutStrategy(nil, now)

	if err := tx.Fulfillments().Create(ctx, &f); err != nil {
		t.Fatal(err)
	}

	dUID := uuid.New().String()
	if err := tx.Deployments().Create(ctx, domain.Deployment{
		ID:            "race-dep",
		UID:           dUID,
		FulfillmentID: fID,
		CreatedAt:     now,
		UpdatedAt:     now,
		Etag:          dUID,
	}); err != nil {
		t.Fatal(err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	return orchWf, fID
}

// TestOrchestration_CleanupBeforeResult verifies that the running-map
// entry is removed BEFORE the result is sent on the done channel.
//
// The original race: the deferred cleanup ran AFTER the result was
// sent, so a caller that received the result and immediately called
// Start() would see ErrAlreadyRunning because the old goroutine's
// defer hadn't executed yet.
func TestOrchestration_CleanupBeforeResult(t *testing.T) {
	orchWf, fID := raceSetup(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for i := range 50 {
		exec, err := orchWf.Start(ctx, fID)
		if err != nil {
			t.Fatalf("iteration %d: Start: %v", i, err)
		}
		if _, err := exec.AwaitResult(ctx); err != nil {
			t.Fatalf("iteration %d: AwaitResult: %v", i, err)
		}
	}
}

// TestOrchestration_DeferDoesNotClobberNewStart verifies that the
// deferred cleanup from a completed goroutine does not delete a
// new Start()'s running-map entry (CodeRabbit's concern about the
// duplicate cleanup at lines 183-187 and 232-235).
func TestOrchestration_DeferDoesNotClobberNewStart(t *testing.T) {
	orchWf, fID := raceSetup(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for i := range 50 {
		exec1, err := orchWf.Start(ctx, fID)
		if err != nil {
			t.Fatalf("iteration %d: first Start: %v", i, err)
		}
		if _, err := exec1.AwaitResult(ctx); err != nil {
			t.Fatalf("iteration %d: first AwaitResult: %v", i, err)
		}

		// Start a new workflow immediately. The old goroutine's
		// defer might not have run yet.
		exec2, err := orchWf.Start(ctx, fID)
		if err != nil {
			t.Fatalf("iteration %d: second Start: %v", i, err)
		}

		// If old defer clobbers the new running entry, a third
		// Start() succeeds (duplicate workflow) instead of
		// returning ErrAlreadyRunning.
		_, err = orchWf.Start(ctx, fID)
		if err == nil {
			t.Fatalf("iteration %d: third Start succeeded — old defer clobbered new running entry", i)
		}
		if !errors.Is(err, domain.ErrAlreadyRunning) {
			t.Fatalf("iteration %d: third Start: %v", i, err)
		}

		if _, err := exec2.AwaitResult(ctx); err != nil {
			t.Fatalf("iteration %d: second AwaitResult: %v", i, err)
		}
	}
}

// TestOrchestration_ConcurrentStartAfterComplete hammers Start from
// multiple goroutines to surface races detectable by -race.
func TestOrchestration_ConcurrentStartAfterComplete(t *testing.T) {
	orchWf, fID := raceSetup(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	for range 5 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 20 {
				exec, err := orchWf.Start(ctx, fID)
				if errors.Is(err, domain.ErrAlreadyRunning) {
					continue
				}
				if err != nil {
					t.Errorf("Start: %v", err)
					return
				}
				if _, err := exec.AwaitResult(ctx); err != nil {
					t.Errorf("AwaitResult: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
}
