package topology

// NetworkTopology models observed network properties across cluster nodes.
type NetworkTopology struct {
	Version            int64                         `json:"version"`
	NodeLatencies      map[string]map[string]float64 `json:"nodeLatencies"`
	NodeBandwidth      map[string]map[string]float64 `json:"nodeBandwidth"`
	AvailableBandwidth map[string]map[string]float64 `json:"availableBandwidth"`
	ZoneMapping        map[string]string             `json:"zoneMapping"`
	RegionMapping      map[string]string             `json:"regionMapping"`
}

// GetLatency returns the latency between two nodes and whether a value exists.
func (nt *NetworkTopology) GetLatency(nodeA, nodeB string) (float64, bool) {
	if nt == nil || nt.NodeLatencies == nil {
		return 0, false
	}
	peers, ok := nt.NodeLatencies[nodeA]
	if !ok {
		return 0, false
	}
	latency, ok := peers[nodeB]
	return latency, ok
}

// GetBandwidth returns the available bandwidth between two nodes and whether a value exists.
func (nt *NetworkTopology) GetBandwidth(nodeA, nodeB string) (float64, bool) {
	if nt == nil || nt.NodeBandwidth == nil {
		return 0, false
	}
	peers, ok := nt.NodeBandwidth[nodeA]
	if !ok {
		return 0, false
	}
	bandwidth, ok := peers[nodeB]
	return bandwidth, ok
}

// GetAvailableBandwidth returns currently available bandwidth for the link.
func (nt *NetworkTopology) GetAvailableBandwidth(nodeA, nodeB string) (float64, bool) {
	if nt == nil || nt.AvailableBandwidth == nil {
		return 0, false
	}
	peers, ok := nt.AvailableBandwidth[nodeA]
	if !ok {
		return 0, false
	}
	bandwidth, ok := peers[nodeB]
	return bandwidth, ok
}

// ReserveBandwidth decrements available bandwidth on a link. It returns false
// when the link has no known capacity or insufficient remaining bandwidth.
func (nt *NetworkTopology) ReserveBandwidth(nodeA, nodeB string, amount float64) bool {
	if nt == nil || amount < 0 {
		return false
	}

	current, ok := nt.currentAvailable(nodeA, nodeB)
	if !ok || current < amount {
		return false
	}

	nt.ensureAvailableMap(nodeA)
	nt.AvailableBandwidth[nodeA][nodeB] = current - amount
	return true
}

// ReleaseBandwidth increments available bandwidth on a link, capping at total
// known capacity when available.
func (nt *NetworkTopology) ReleaseBandwidth(nodeA, nodeB string, amount float64) bool {
	if nt == nil || amount < 0 {
		return false
	}

	current, ok := nt.currentAvailable(nodeA, nodeB)
	if !ok {
		return false
	}

	next := current + amount
	if total, ok := nt.GetBandwidth(nodeA, nodeB); ok && next > total {
		next = total
	}

	nt.ensureAvailableMap(nodeA)
	nt.AvailableBandwidth[nodeA][nodeB] = next
	return true
}

func (nt *NetworkTopology) ensureAvailableMap(nodeA string) {
	if nt.AvailableBandwidth == nil {
		nt.AvailableBandwidth = make(map[string]map[string]float64)
	}
	if _, ok := nt.AvailableBandwidth[nodeA]; !ok {
		nt.AvailableBandwidth[nodeA] = make(map[string]float64)
	}
}

func (nt *NetworkTopology) currentAvailable(nodeA, nodeB string) (float64, bool) {
	if available, ok := nt.GetAvailableBandwidth(nodeA, nodeB); ok {
		return available, true
	}
	if total, ok := nt.GetBandwidth(nodeA, nodeB); ok {
		nt.ensureAvailableMap(nodeA)
		nt.AvailableBandwidth[nodeA][nodeB] = total
		return total, true
	}
	return 0, false
}

// GetZone returns the zone for a node and whether a value exists.
func (nt *NetworkTopology) GetZone(nodeID string) (string, bool) {
	if nt == nil || nt.ZoneMapping == nil {
		return "", false
	}
	zone, ok := nt.ZoneMapping[nodeID]
	return zone, ok
}

// GetRegion returns the region for a node and whether a value exists.
func (nt *NetworkTopology) GetRegion(nodeID string) (string, bool) {
	if nt == nil || nt.RegionMapping == nil {
		return "", false
	}
	region, ok := nt.RegionMapping[nodeID]
	return region, ok
}
