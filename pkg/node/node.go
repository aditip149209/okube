package node

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/aditip149209/okube/pkg/utils"
	"github.com/aditip149209/okube/pkg/worker"
)

//a node is an object that represents a machine in the cluster. The manager is a type of node, and so is the worker.
//the worker and manager represent the logical workload of the orchestrator, while the node deals with the physical layer.
//the worker will need to collect stats, which can be done using the node struct.
//similarly for manager, for its own purposes. when we want to scale our manager to multiple nodes, then node becomes even more important for

type Node struct {
	Name            string
	Ip              string
	Cores           int
	Memory          int64
	MemoryAllocated int
	Disk            int64
	Stats           worker.Stats
	DiskAllocated   int
	Role            string
	TaskCount       int
	Cpu             int
}

type Option func(*Node)

func WithCore(c int) Option {
	return func(n *Node) {
		n.Cores = c
	}
}

func WithMemory(m int) Option {
	return func(n *Node) {
		n.Memory = int64(m)
	}
}

func WithDisk(d int) Option {
	return func(n *Node) {
		n.Disk = int64(d)
	}
}

func WithCpu(c int) Option {
	return func(n *Node) {
		n.Cpu = c
	}
}

// NewNode creates and returns a new Node instance
func NewNode(name string, ip string, role string, opts ...Option) *Node {

	n := &Node{
		Name:            name,
		Ip:              ip,
		MemoryAllocated: 0,
		DiskAllocated:   0,
		Role:            role,
		TaskCount:       0,
		Cpu:             0,
	}

	for _, opt := range opts {
		opt(n)
	}

	return n
}

func (n *Node) GetNodeStats() (*worker.Stats, error) {
	var resp *http.Response
	var err error

	url := fmt.Sprintf("http://%s/stats", n.Ip)
	resp, err = utils.HTTPWithRetry(http.Get, url)

	if err != nil {
		msg := fmt.Sprintf("Unable to connect to %v. Permanent failure.\n", n.Ip)
		log.Println(msg)
		return nil, errors.New(msg)
	}

	if resp.StatusCode != 200 {
		msg := fmt.Sprintf("Error retrieving stats from %v: %v", n.Ip, err)
		log.Println(msg)
		return nil, errors.New(msg)

	}

	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	var stats worker.Stats

	err = json.Unmarshal(body, &stats)
	if err != nil {
		msg := fmt.Sprintf("error decoding message while getting stats for node %s", n.Name)
		log.Println(msg)
		return nil, errors.New(msg)
	}

	n.Memory = int64(stats.MemTotalKb())
	n.Disk = int64(stats.DiskTotal())

	n.Stats = stats

	return &n.Stats, nil

}
