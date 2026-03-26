package scheduler

import (
	"fmt"
	"sync"

	"github.com/aditip149209/okube/pkg/appgroup"
	"github.com/aditip149209/okube/pkg/node"
	"github.com/aditip149209/okube/pkg/task"
)

// QueueSortCapable marks schedulers that can order pending tasks before
// placement, equivalent to the QueueSort extension point.
type QueueSortCapable interface {
	QueueSort(tasks []*task.Task, groups []*appgroup.AppGroup) []*task.Task
}

type ResourceFilterPlugin interface {
	Filter(t task.Task, nodes []*node.Node) []*node.Node
}

type NetworkFilterPlugin interface {
	Filter(t task.Task, nodes []*node.Node, filterCtx *FilterContext) []*node.Node
}

type ResourceScorePlugin interface {
	Score(t task.Task, nodes []*node.Node) map[string]float64
}

type NetworkScorePlugin interface {
	Score(t task.Task, nodes []*node.Node, resourceScores map[string]float64, scoreCtx *ScoreContext) map[string]float64
}

type FinalSelectionPlugin interface {
	Pick(scores map[string]float64, candidates []*node.Node) *node.Node
}

// ComposableScheduler executes scheduling in ordered plugin stages:
// QueueSort -> ResourceFilter -> NetworkFilter -> ResourceScore ->
// NetworkScore -> FinalSelection.
type ComposableScheduler struct {
	queueSorter    *QueueSorter
	resourceFilter ResourceFilterPlugin
	networkFilter  NetworkFilterPlugin
	resourceScore  ResourceScorePlugin
	networkScore   NetworkScorePlugin
	finalSelect    FinalSelectionPlugin
}

func NewPipelineScheduler(schedulerType, queueSortStrategy string) Scheduler {
	resourceFilter := ResourceFilterPlugin(roundRobinResourceFilter{})
	resourceScore := ResourceScorePlugin(newRoundRobinResourceScorer())

	switch schedulerType {
	case "epvm":
		resourceFilter = epvmResourceFilter{}
		resourceScore = epvmResourceScorer{}
	}

	return &ComposableScheduler{
		queueSorter:    NewQueueSorter(queueSortStrategy),
		resourceFilter: resourceFilter,
		networkFilter:  networkFilterPlugin{},
		resourceScore:  resourceScore,
		networkScore:   networkScorePlugin{},
		finalSelect:    lowestScoreSelector{},
	}
}

func (c *ComposableScheduler) QueueSort(tasks []*task.Task, groups []*appgroup.AppGroup) []*task.Task {
	if c == nil || c.queueSorter == nil {
		return tasks
	}
	return c.queueSorter.SortPendingTasks(tasks, groups)
}

func (c *ComposableScheduler) SelectCandidateNodes(t task.Task, nodes []*node.Node, filterCtx *FilterContext) []*node.Node {
	if c == nil {
		return nil
	}

	// 2) Resource feasibility filter.
	resourceCandidates := c.resourceFilter.Filter(t, nodes)
	// 3) Network filter.
	return c.networkFilter.Filter(t, resourceCandidates, filterCtx)
}

func (c *ComposableScheduler) Score(t task.Task, nodes []*node.Node, scoreCtx *ScoreContext) map[string]float64 {
	if c == nil {
		return map[string]float64{}
	}

	// 4) Resource scoring.
	resourceScores := c.resourceScore.Score(t, nodes)
	// 5) Network scoring.
	return c.networkScore.Score(t, nodes, resourceScores, scoreCtx)
}

func (c *ComposableScheduler) Pick(scores map[string]float64, candidates []*node.Node) *node.Node {
	if c == nil {
		return nil
	}
	// 6) Final selection.
	return c.finalSelect.Pick(scores, candidates)
}

type roundRobinResourceFilter struct{}

func (roundRobinResourceFilter) Filter(t task.Task, nodes []*node.Node) []*node.Node {
	return nodes
}

type epvmResourceFilter struct{}

func (epvmResourceFilter) Filter(t task.Task, nodes []*node.Node) []*node.Node {
	var candidates []*node.Node
	for i := range nodes {
		if checkDisk(t, nodes[i].Disk-int64(nodes[i].DiskAllocated)) {
			candidates = append(candidates, nodes[i])
		}
	}
	return candidates
}

type roundRobinResourceScorer struct {
	mu         sync.Mutex
	lastWorker int
}

func newRoundRobinResourceScorer() *roundRobinResourceScorer {
	return &roundRobinResourceScorer{lastWorker: 0}
}

func (r *roundRobinResourceScorer) Score(t task.Task, nodes []*node.Node) map[string]float64 {
	nodeScores := make(map[string]float64)
	if len(nodes) == 0 {
		return nodeScores
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	newWorker := 0
	if r.lastWorker+1 < len(nodes) {
		newWorker = r.lastWorker + 1
		r.lastWorker++
	} else {
		r.lastWorker = 0
	}

	for idx, n := range nodes {
		if idx == newWorker {
			nodeScores[n.Name] = 0.1
		} else {
			nodeScores[n.Name] = 1.0
		}
	}
	return nodeScores
}

type epvmResourceScorer struct{}

func (epvmResourceScorer) Score(t task.Task, nodes []*node.Node) map[string]float64 {
	// Reuse existing EPVM resource scoring logic by calling the legacy scorer and
	// stripping network context from this phase.
	legacy := &Epvm{Name: "epvm"}
	return legacy.rawResourceScores(t, nodes)
}

type networkFilterPlugin struct{}

func (networkFilterPlugin) Filter(t task.Task, nodes []*node.Node, filterCtx *FilterContext) []*node.Node {
	return filterNodesByNetworkConstraints(t, nodes, filterCtx)
}

type networkScorePlugin struct{}

func (networkScorePlugin) Score(t task.Task, nodes []*node.Node, resourceScores map[string]float64, scoreCtx *ScoreContext) map[string]float64 {
	return applyNetworkAwareScore(t, nodes, resourceScores, scoreCtx)
}

type lowestScoreSelector struct{}

func (lowestScoreSelector) Pick(scores map[string]float64, candidates []*node.Node) *node.Node {
	var bestNode *node.Node
	var lowest float64
	for idx, n := range candidates {
		if idx == 0 {
			bestNode = n
			lowest = scores[n.Name]
			continue
		}
		if scores[n.Name] < lowest {
			lowest = scores[n.Name]
			bestNode = n
		}
	}
	return bestNode
}

func (c *ComposableScheduler) String() string {
	return fmt.Sprintf("ComposableScheduler(%T)", c.resourceScore)
}
