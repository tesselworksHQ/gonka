package broker

// CI-only (not part of the PR): gafrustam's stress tests from the #2318 review,
// env skip removed (run with -cpu=2, one test per process), plus the PR's
// selectInferenceModel race test, against the PR head before the follow-up fix.

import (
	"decentralized-api/apiconfig"
	"decentralized-api/participant"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/productscience/inference/x/inference/types"
	"github.com/stretchr/testify/require"
)

const stressFor = 20 * time.Second

func stressBroker() (*Broker, *NodeWithState) {
	live := &NodeWithState{
		Node: Node{Id: "node-1", Models: map[string]ModelArgs{"m1": {}}},
		State: NodeState{
			IntendedStatus:    types.HardwareNodeStatus_POC,
			PocIntendedStatus: PocStatusGenerating,
			EpochModels:       map[string]types.Model{"m1": {Id: "m1"}},
			EpochMLNodes:      map[string]types.MLNodeInfo{"m1": {NodeId: "node-1"}},
			PreservedModels:   map[string]bool{},
		},
	}
	b := &Broker{
		nodes:           map[string]*NodeWithState{"node-1": live},
		participantInfo: participant.CosmosInfo{Address: "cosmos1dummyaddress", PubKey: "dummyPubKey"},
	}
	return b, live
}

func stress(t *testing.T, b *Broker, read func()) {
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.Level(100)})))
	defer slog.SetDefault(prev)

	stop := make(chan struct{})
	done := make(chan struct{})
	writes := 0
	go func() {
		defer close(done)
		mlNodes := []*types.MLNodeInfo{{NodeId: "node-1"}}
		for {
			select {
			case <-stop:
				return
			default:
			}
			b.UpdateNodeEpochData(mlNodes, "m1", types.Model{Id: "m1"})
			writes++
		}
	}()

	start := time.Now()
	reads := 0
	for time.Since(start) < stressFor {
		for i := 0; i < 1000; i++ {
			read()
		}
		reads += 1000
	}
	close(stop)
	<-done
	t.Logf("no crash: %d reads, %d writes in %s", reads, writes, time.Since(start).Round(time.Millisecond))
}

func TestEpochMapsStress_SelectInferenceModel(t *testing.T) {
	b, live := stressBroker()
	w := &NodeWorker{nodeId: "node-1", node: live, broker: b}
	stress(t, b, func() {
		_, _ = selectInferenceModel(w)
	})
}

// Reconcile path exactly as on the PR head: plain struct copy under b.mu.
func TestEpochMapsStress_ReconcileGetCommandForState(t *testing.T) {
	b, live := stressBroker()
	params := &pocParams{
		startPoCBlockHeight: 100,
		models:              map[string]apiconfig.PoCModelConfigCache{"m1": {ModelId: "m1"}},
	}
	b.mu.Lock()
	stateForCmd := live.State
	modelsForCmd := live.Node.Models
	b.mu.Unlock()
	if cmd := b.getCommandForState("node-1", &stateForCmd, modelsForCmd, params, nil, 1, nil); cmd == nil {
		t.Fatal("expected a StartPoCNodeCommandV2")
	}
	stress(t, b, func() {
		_ = b.getCommandForState("node-1", &stateForCmd, modelsForCmd, params, nil, 1, nil)
	})
}

// Same test as in the PR's get_nodes_copy_test.go.
func TestSelectInferenceModel_ConcurrentEpochUpdate(t *testing.T) {
	b, live := stressBroker()
	w := &NodeWorker{nodeId: "node-1", node: live, broker: b}

	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	defer slog.SetDefault(prev)

	const rounds = 2000
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		mlNodes := []*types.MLNodeInfo{{NodeId: "node-1"}}
		for i := 0; i < rounds; i++ {
			b.UpdateNodeEpochData(mlNodes, "m1", types.Model{Id: "m1"})
		}
	}()
	for i := 0; i < rounds; i++ {
		model, err := selectInferenceModel(w)
		require.NoError(t, err)
		require.Equal(t, "m1", model.Id)
	}
	wg.Wait()
}
