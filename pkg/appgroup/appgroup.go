package appgroup

import (
	"errors"
	"fmt"
)

// DependencyEdge represents a directed dependency from one service to another,
// with optional QoS constraints.
type DependencyEdge struct {
	From           string   `json:"from"`                     // source service ID (the dependent)
	To             string   `json:"to"`                       // target service ID (the dependency)
	MinBandwidth   *float64 `json:"minBandwidth,omitempty"`   // optional minimum bandwidth requirement in Mbps
	MaxNetworkCost *float64 `json:"maxNetworkCost,omitempty"` // optional maximum network cost (abstract unit)
}

// AppGroup represents an application composed of multiple services (tasks)
// with directed dependency edges forming a DAG.
type AppGroup struct {
	AppID    string           `json:"appID"`
	Services []string         `json:"services"`
	Edges    []DependencyEdge `json:"edges"`

	// Forward adjacency: service → edges where it is the dependent (From).
	deps map[string][]DependencyEdge
	// Reverse adjacency: service → edges where it is the dependency (To).
	rdeps map[string][]DependencyEdge
	// Fast lookup for valid service IDs.
	serviceSet map[string]struct{}
}

// NewAppGroup creates an AppGroup after validating that all edge endpoints
// reference known services and the graph contains no cycles.
func NewAppGroup(appID string, services []string, edges []DependencyEdge) (*AppGroup, error) {
	if appID == "" {
		return nil, errors.New("appID cannot be empty")
	}

	serviceSet := make(map[string]struct{}, len(services))
	for _, s := range services {
		if s == "" {
			return nil, errors.New("service ID cannot be empty")
		}
		if _, exists := serviceSet[s]; exists {
			return nil, fmt.Errorf("duplicate service ID %q", s)
		}
		serviceSet[s] = struct{}{}
	}

	// Validate edge endpoints.
	for _, e := range edges {
		if e.From == "" || e.To == "" {
			return nil, errors.New("edge endpoints cannot be empty")
		}
		if _, ok := serviceSet[e.From]; !ok {
			return nil, fmt.Errorf("unknown service %q in edge From", e.From)
		}
		if _, ok := serviceSet[e.To]; !ok {
			return nil, fmt.Errorf("unknown service %q in edge To", e.To)
		}
	}

	deps := make(map[string][]DependencyEdge, len(services))
	rdeps := make(map[string][]DependencyEdge, len(services))
	for _, s := range services {
		deps[s] = nil
		rdeps[s] = nil
	}
	for _, e := range edges {
		deps[e.From] = append(deps[e.From], e)
		rdeps[e.To] = append(rdeps[e.To], e)
	}

	ag := &AppGroup{
		AppID:      appID,
		Services:   services,
		Edges:      edges,
		deps:       deps,
		rdeps:      rdeps,
		serviceSet: serviceSet,
	}

	// Cycle detection.
	if err := ag.detectCycle(); err != nil {
		return nil, err
	}

	return ag, nil
}

// GetDependencies returns the edges where serviceID is the dependent (From),
// i.e. the services that serviceID depends on.
func (ag *AppGroup) GetDependencies(serviceID string) []DependencyEdge {
	return ag.deps[serviceID]
}

// GetDependents returns the edges where serviceID is the dependency (To),
// i.e. the services that depend on serviceID.
func (ag *AppGroup) GetDependents(serviceID string) []DependencyEdge {
	return ag.rdeps[serviceID]
}

// TopologicalOrder returns the services in a valid topological ordering
// using Kahn's algorithm. Returns an error if the graph contains a cycle.
func (ag *AppGroup) TopologicalOrder() ([]string, error) {
	inDegree := make(map[string]int, len(ag.Services))
	for _, s := range ag.Services {
		inDegree[s] = 0
	}
	for _, e := range ag.Edges {
		inDegree[e.To]++
	}

	queue := make([]string, 0)
	for _, s := range ag.Services {
		if inDegree[s] == 0 {
			queue = append(queue, s)
		}
	}

	order := make([]string, 0, len(ag.Services))
	for len(queue) > 0 {
		curr := queue[0]
		queue = queue[1:]
		order = append(order, curr)

		for _, e := range ag.deps[curr] {
			inDegree[e.To]--
			if inDegree[e.To] == 0 {
				queue = append(queue, e.To)
			}
		}
	}

	if len(order) != len(ag.Services) {
		return nil, errors.New("cycle detected in dependency graph")
	}

	return order, nil
}

// detectCycle performs a DFS-based cycle detection on the dependency graph.
func (ag *AppGroup) detectCycle() error {
	const (
		white = 0 // unvisited
		gray  = 1 // in current DFS path
		black = 2 // fully processed
	)

	color := make(map[string]int, len(ag.Services))
	for _, s := range ag.Services {
		color[s] = white
	}

	var dfs func(string) error
	dfs = func(u string) error {
		color[u] = gray
		for _, e := range ag.deps[u] {
			switch color[e.To] {
			case gray:
				return fmt.Errorf("cycle detected: edge %s -> %s", e.From, e.To)
			case white:
				if err := dfs(e.To); err != nil {
					return err
				}
			}
		}
		color[u] = black
		return nil
	}

	for _, s := range ag.Services {
		if color[s] == white {
			if err := dfs(s); err != nil {
				return err
			}
		}
	}

	return nil
}
