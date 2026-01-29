package node

//a node is an object that represents a machine in the cluster. The manager is a type of node, and so is the worker.
//the worker and manager represent the logical workload of the orchestrator, while the node deals with the physical layer.
//the worker will need to collect stats, which can be done using the node struct.
//similarly for manager, for its own purposes. when we want to scale our manager to multiple nodes, then node becomes even more important for

type Node struct {
	Name            string
	Ip              string
	Cores           int
	Memory          int
	MemoryAllocated int
	Disk            int
	DiskAllocated   int
	Role            string
	TaskCount       int
}

type Option func(*Node)

func WithCore(c int) Option {
	return func(n *Node) {
		n.Cores = c
	}
}

func WithMemory(m int) Option {
	return func(n *Node) {
		n.Memory = m
	}
}

func WithDisk(d int) Option {
	return func(n *Node) {
		n.Disk = d
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
	}

	for _, opt := range opts {
		opt(n)
	}

	return n
}
