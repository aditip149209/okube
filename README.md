# Okube

A lightweight, network-aware container orchestrator inspired by Kubernetes, designed for on-premise server architectures.

## Overview

Okube is a container orchestration system that manages containerized workloads across a cluster of nodes. Built with Go and Docker, it provides a simplified alternative to Kubernetes for on-premise deployments, offering task scheduling, resource management, and automated container lifecycle management.

## Features

- **Container Orchestration**: Manage containerized applications using Docker
- **Task Scheduling**: Intelligent task placement across worker nodes
- **Resource Management**: Track and allocate CPU, memory, and disk resources
- **Worker-Manager Architecture**: Distributed system with clear separation of concerns
- **CLI Interface**: User-friendly command-line tool for cluster management
- **State Management**: Track task states (Pending, Scheduled, Running, Completed, Failed)
- **Network Awareness**: Designed with network topology in mind for on-premise environments

## Architecture

### Components

- **Manager**: Orchestrates task distribution, maintains cluster state, and makes scheduling decisions
- **Worker**: Executes tasks on individual nodes, reports resource usage and task status
- **Scheduler**: Selects optimal nodes for task placement based on resource availability
- **Node**: Represents physical or virtual machines in the cluster
- **Task**: Unit of work representing a containerized application

### Project Structure

```
okube/
├── cmd/
│   └── cli/              # Command-line interface
│       ├── main.go       # CLI entry point
│       ├── run.go        # Run command implementation
│       └── apply.go      # Apply command implementation
├── pkg/
│   ├── manager/          # Manager component
│   ├── worker/           # Worker component
│   ├── scheduler/        # Scheduling logic
│   ├── task/             # Task definitions and Docker operations
│   └── node/             # Node representation
├── main.go               # Main application entry point
└── go.mod                # Go module dependencies
```

## Installation

### Prerequisites

- Go 1.25.3 or higher
- Docker installed and running
- Linux operating system (recommended)

### Build from Source

```bash
# Clone the repository
git clone https://github.com/aditip149209/okube.git
cd okube

# Install dependencies
go mod download

# Build the binary
go build -o okube main.go

# Build the CLI
go build -o okube-cli cmd/cli/main.go
```

## Usage

### Starting the Orchestrator

Run the main orchestrator:

```bash
./okube
```

### Using the CLI

The CLI provides commands to interact with the cluster:

#### Run a Container

Start a new task on the cluster:

```bash
./okube-cli run --image nginx --replicas 3
```

Options:
- `-i, --image`: Container image to run (required)
- `-r, --replicas`: Number of replicas to start (default: 1)

#### Apply Configuration

```bash
./okube-cli apply
```

## API Reference

### Task States

Tasks progress through the following states:

- `Pending`: Task created but not yet scheduled
- `Scheduled`: Task assigned to a worker node
- `Running`: Task container is active
- `Completed`: Task finished successfully
- `Failed`: Task encountered an error

### Task Configuration

```go
type Task struct {
    ID            uuid.UUID
    Name          string
    State         State
    Image         string
    Memory        int
    Disk          int
    ExposedPorts  nat.PortSet
    PortBindings  map[string]string
    RestartPolicy string
    StartTime     time.Time
    EndTime       time.Time
}
```

### Node Configuration

```go
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
```

## Development

### Dependencies

Key dependencies include:
- **Docker SDK**: `github.com/docker/docker/client`
- **Cobra**: CLI framework
- **UUID**: Unique identifier generation
- **Go Collections**: Queue implementation

### Running Tests

```bash
go test ./...
```

### Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## Roadmap

- [ ] HTTP API server for remote management
- [ ] Advanced scheduling algorithms
- [ ] Health checks and auto-recovery
- [ ] Resource quotas and limits
- [ ] Multi-manager high availability
- [ ] Service discovery and networking
- [ ] Persistent volume support
- [ ] Configuration file support (YAML/JSON)
- [ ] Monitoring and metrics collection
- [ ] Web-based dashboard

## Technical Details

### Resource Management

Each node tracks:
- CPU cores (available and allocated)
- Memory (total and allocated)
- Disk space (total and allocated)
- Running task count

### Docker Integration

Okube uses the Docker SDK to:
- Pull container images
- Create and start containers
- Configure resource limits (CPU, memory)
- Set up port bindings and environment variables
- Manage container lifecycle (start, stop, remove)

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Author

**Aditi P** - [aditip149209](https://github.com/aditip149209)

## Acknowledgments

- Inspired by Kubernetes architecture
- Built as a learning project for distributed systems and container orchestration
- Uses the Docker Engine API for container management

## Support

For issues, questions, or contributions, please open an issue on the [GitHub repository](https://github.com/aditip149209/okube).

---

**Note**: This is an educational project and is not recommended for production use. For production container orchestration, consider using Kubernetes, Docker Swarm, or other mature platforms.


<!-- 
todo: cloud provider interface, multiple managers using raft, network aware capabilities, custom network aware scheduler plugin for kubernetes to be donated -->

