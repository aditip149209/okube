package scheduler

import (
	"github.com/aditip149209/okube/pkg/appgroup"
	"github.com/aditip149209/okube/pkg/node"
	"github.com/aditip149209/okube/pkg/task"
)

func filterNodesByNetworkConstraints(t task.Task, candidates []*node.Node, filterCtx *FilterContext) []*node.Node {
	if len(candidates) == 0 || filterCtx == nil || filterCtx.AppGroup == nil || filterCtx.NetworkTopology == nil {
		return candidates
	}

	serviceID := t.ServiceID
	if serviceID == "" {
		serviceID = t.Name
	}
	if serviceID == "" {
		return candidates
	}

	edges := filterCtx.AppGroup.GetDependencies(serviceID)
	if len(edges) == 0 {
		return candidates
	}

	accepted := make([]*node.Node, 0, len(candidates))
	for _, candidate := range candidates {
		if candidate == nil {
			continue
		}
		if violatesHardNetworkConstraint(candidate.Name, edges, filterCtx) {
			continue
		}
		accepted = append(accepted, candidate)
	}

	return accepted
}

func violatesHardNetworkConstraint(candidateNodeID string, edges []appgroup.DependencyEdge, filterCtx *FilterContext) bool {
	for _, edge := range edges {
		depNodeID, ok := filterCtx.DependencyNodeByService[edge.To]
		if !ok || depNodeID == "" {
			continue
		}

		if edge.MaxNetworkCost != nil {
			if latency, ok := filterCtx.NetworkTopology.GetLatency(candidateNodeID, depNodeID); ok {
				if latency > *edge.MaxNetworkCost {
					return true
				}
			}
		}

		if edge.MinBandwidth != nil {
			bandwidth, ok := filterCtx.NetworkTopology.GetAvailableBandwidth(candidateNodeID, depNodeID)
			if !ok {
				bandwidth, ok = filterCtx.NetworkTopology.GetBandwidth(candidateNodeID, depNodeID)
			}
			if ok {
				if bandwidth < *edge.MinBandwidth {
					return true
				}
			}
		}
	}

	return false
}
