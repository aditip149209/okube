package topology

import (
	"context"
	"math/rand"
	"sort"
	"strings"
	"time"
)

type ProbeMode string

const (
	ProbeModeFullMesh ProbeMode = "full-mesh"
	ProbeModeSampled  ProbeMode = "sampled"
)

type NodeTarget struct {
	ID      string
	Address string
}

type Store interface {
	GetNetworkTopology(ctx context.Context) (*NetworkTopology, error)
	SaveNetworkTopology(ctx context.Context, nt *NetworkTopology) error
}

type NodeLister func(ctx context.Context) ([]NodeTarget, error)
type LatencyProbe func(ctx context.Context, from NodeTarget, to NodeTarget) (float64, error)

type UpdaterConfig struct {
	Store      Store
	ListNodes  NodeLister
	Probe      LatencyProbe
	Mode       ProbeMode
	Interval   time.Duration
	SampleSize int
}

type Updater struct {
	cfg UpdaterConfig
	rng *rand.Rand
}

func ParseProbeMode(raw string) ProbeMode {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case string(ProbeModeSampled):
		return ProbeModeSampled
	default:
		return ProbeModeFullMesh
	}
}

func NewUpdater(cfg UpdaterConfig) *Updater {
	if cfg.Interval <= 0 {
		cfg.Interval = 30 * time.Second
	}
	if cfg.Mode == "" {
		cfg.Mode = ProbeModeFullMesh
	}
	if cfg.SampleSize <= 0 {
		cfg.SampleSize = 2
	}

	return &Updater{
		cfg: cfg,
		rng: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (u *Updater) Run(ctx context.Context) {
	if u == nil || u.cfg.Store == nil || u.cfg.ListNodes == nil || u.cfg.Probe == nil {
		return
	}

	// Prime topology quickly on startup.
	_ = u.UpdateOnce(ctx)

	ticker := time.NewTicker(u.cfg.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = u.UpdateOnce(ctx)
		}
	}
}

func (u *Updater) UpdateOnce(ctx context.Context) error {
	nodes, err := u.cfg.ListNodes(ctx)
	if err != nil {
		return err
	}
	if len(nodes) == 0 {
		return nil
	}

	current, err := u.cfg.Store.GetNetworkTopology(ctx)
	if err != nil || current == nil {
		current = &NetworkTopology{}
	}
	if current.NodeLatencies == nil {
		current.NodeLatencies = make(map[string]map[string]float64)
	}

	pairs := u.selectProbePairs(nodes)
	for _, p := range pairs {
		if p.from.ID == p.to.ID {
			u.setLatency(current, p.from.ID, p.to.ID, 0)
			continue
		}

		latency, probeErr := u.cfg.Probe(ctx, p.from, p.to)
		if probeErr != nil {
			continue
		}
		u.setLatency(current, p.from.ID, p.to.ID, latency)
	}

	current.Version++
	return u.cfg.Store.SaveNetworkTopology(ctx, current)
}

type probePair struct {
	from NodeTarget
	to   NodeTarget
}

func (u *Updater) selectProbePairs(nodes []NodeTarget) []probePair {
	mode := ParseProbeMode(string(u.cfg.Mode))
	if mode == ProbeModeSampled {
		return u.selectSampledPairs(nodes, u.cfg.SampleSize)
	}
	return u.selectFullMeshPairs(nodes)
}

func (u *Updater) selectFullMeshPairs(nodes []NodeTarget) []probePair {
	pairs := make([]probePair, 0, len(nodes)*len(nodes))
	for _, from := range nodes {
		for _, to := range nodes {
			pairs = append(pairs, probePair{from: from, to: to})
		}
	}
	return pairs
}

func (u *Updater) selectSampledPairs(nodes []NodeTarget, sampleSize int) []probePair {
	pairs := make([]probePair, 0, len(nodes)*(sampleSize+1))
	for _, from := range nodes {
		// always include self latency baseline
		pairs = append(pairs, probePair{from: from, to: from})

		targets := make([]NodeTarget, 0, len(nodes)-1)
		for _, to := range nodes {
			if to.ID != from.ID {
				targets = append(targets, to)
			}
		}

		if len(targets) == 0 {
			continue
		}

		sort.Slice(targets, func(i, j int) bool { return targets[i].ID < targets[j].ID })
		u.rng.Shuffle(len(targets), func(i, j int) { targets[i], targets[j] = targets[j], targets[i] })

		limit := sampleSize
		if limit > len(targets) {
			limit = len(targets)
		}

		for i := 0; i < limit; i++ {
			pairs = append(pairs, probePair{from: from, to: targets[i]})
		}
	}
	return pairs
}

func (u *Updater) setLatency(nt *NetworkTopology, fromNodeID, toNodeID string, latency float64) {
	if nt.NodeLatencies == nil {
		nt.NodeLatencies = make(map[string]map[string]float64)
	}
	if _, ok := nt.NodeLatencies[fromNodeID]; !ok {
		nt.NodeLatencies[fromNodeID] = make(map[string]float64)
	}
	nt.NodeLatencies[fromNodeID][toNodeID] = latency
}
