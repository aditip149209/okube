package scheduler

import (
	"math"

	"github.com/aditip149209/okube/pkg/node"
	"github.com/aditip149209/okube/pkg/task"
)

const (
	defaultNetworkWeight  = 0.7
	defaultResourceWeight = 0.3
)

func applyNetworkAwareScore(t task.Task, nodes []*node.Node, resourceScores map[string]float64, scoreCtx *ScoreContext) map[string]float64 {
	if len(nodes) == 0 {
		return resourceScores
	}

	if scoreCtx == nil || scoreCtx.Filter == nil || scoreCtx.Filter.AppGroup == nil || scoreCtx.Filter.NetworkTopology == nil {
		return resourceScores
	}

	serviceID := serviceIDForTask(&t)
	if serviceID == "" {
		return resourceScores
	}

	edges := scoreCtx.Filter.AppGroup.GetDependencies(serviceID)
	if len(edges) == 0 {
		return resourceScores
	}

	networkCosts := make(map[string]float64, len(nodes))
	hasNetworkData := false
	for _, n := range nodes {
		if n == nil {
			continue
		}
		cost := 0.0
		usedMetric := false
		for _, edge := range edges {
			depNodeID, ok := scoreCtx.Filter.DependencyNodeByService[edge.To]
			if !ok || depNodeID == "" {
				continue
			}
			latency, ok := scoreCtx.Filter.NetworkTopology.GetLatency(n.Name, depNodeID)
			if !ok {
				continue
			}
			cost += latency
			usedMetric = true
		}
		if usedMetric {
			hasNetworkData = true
		}
		networkCosts[n.Name] = cost
	}

	if !hasNetworkData {
		return resourceScores
	}

	networkScores := normalizeScores(nodes, networkCosts)
	resourceNormalized := normalizeScores(nodes, resourceScores)
	networkWeight, resourceWeight := resolveScoreWeights(scoreCtx)

	final := make(map[string]float64, len(resourceScores))
	for _, n := range nodes {
		if n == nil {
			continue
		}
		final[n.Name] = networkWeight*networkScores[n.Name] + resourceWeight*resourceNormalized[n.Name]
	}

	return final
}

func normalizeScores(nodes []*node.Node, raw map[string]float64) map[string]float64 {
	normalized := make(map[string]float64, len(raw))
	if len(nodes) == 0 {
		return normalized
	}

	minV := math.MaxFloat64
	maxV := -math.MaxFloat64
	for _, n := range nodes {
		if n == nil {
			continue
		}
		v := raw[n.Name]
		if v < minV {
			minV = v
		}
		if v > maxV {
			maxV = v
		}
	}

	if minV == math.MaxFloat64 {
		return normalized
	}

	if maxV == minV {
		for _, n := range nodes {
			if n == nil {
				continue
			}
			normalized[n.Name] = 0
		}
		return normalized
	}

	rangeV := maxV - minV
	for _, n := range nodes {
		if n == nil {
			continue
		}
		v := raw[n.Name]
		normalized[n.Name] = ((v - minV) / rangeV) * 100.0
	}

	return normalized
}

func resolveScoreWeights(scoreCtx *ScoreContext) (float64, float64) {
	nw := defaultNetworkWeight
	rw := defaultResourceWeight
	if scoreCtx != nil {
		if scoreCtx.NetworkWeight > 0 {
			nw = scoreCtx.NetworkWeight
		}
		if scoreCtx.ResourceWeight > 0 {
			rw = scoreCtx.ResourceWeight
		}
	}

	sum := nw + rw
	if sum <= 0 {
		return defaultNetworkWeight, defaultResourceWeight
	}
	return nw / sum, rw / sum
}
