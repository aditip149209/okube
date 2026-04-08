# Okube Architecture

Okube is a lightweight container orchestrator written in Go, designed for deploying multi-service applications across multiple nodes (laptops/servers) on a local network. Think k3s but simpler — no overlay network, no CRI, no CNI. Just Docker containers coordinated via a central manager with etcd for state.

## Components

### Manager

The central brain of the cluster. Responsibilities:

- **Leader election** via etcd (multiple managers can run for HA; only the leader schedules)
- **Task scheduling** — selects the best worker node for each container using pluggable schedulers
- **App deployment** — deploys multi-service apps in dependency order with automatic service discovery
- **Health checking** — periodically calls health endpoints on running containers; restarts on failure
- **Task state sync** — polls workers for container status updates and persists to etcd
- **Network topology probing** — measures latency between workers for network-aware scheduling

### Worker

Runs on each node (laptop). Responsibilities:

- **Executes containers** via the Docker API (pull image, create, start, stop, remove)
- **Reports stats** — CPU, memory, disk usage sampled every 5 seconds
- **Heartbeat** — sends periodic heartbeats to the manager to prove liveness
- **Serves HTTP API** — the manager communicates with workers via REST (start/stop/list tasks, get stats)

### Scheduler

Selects which worker runs a given task. Pluggable pipeline architecture:

1. **QueueSort** — orders pending tasks (Kahn/ReverseKahn/AlternateKahn topological sort)
2. **ResourceFilter** — eliminates nodes that can't meet resource requirements
3. **NetworkFilter** — eliminates nodes that violate latency/bandwidth constraints
4. **ResourceScore** — scores nodes by CPU/memory/disk load
5. **NetworkScore** — scores nodes by latency to dependency services
6. **Pick** — selects the lowest-score (best) node

Two built-in schedulers: `roundrobin` (simple) and `epvm` (resource-aware with exponential cost model).

### Store (etcd)

All cluster state is persisted in etcd:

- Tasks and their states (`/tasks/{id}`, `/tasks/{id}/state`, `/tasks/{id}/worker`)
- Worker registrations and heartbeats (`/workers/{id}`)
- App records (`/apps/{name}`)
- AppGroup dependency graphs (`/appgroups/{id}`)
- Network topology snapshots (`/network/topology`)
- Leader election key (`/managers/leader`)

### CLI

The `okube` binary is both server and client:

- `okube manager` — starts a manager node
- `okube worker` — starts a worker node
- `okube deploy -f manifest.yaml` — deploys a multi-service app
- `okube apps` — lists deployed applications
- `okube delete <app-name>` — tears down an app
- `okube run -f task.json` — submits a single task
- `okube stop <task-id>` — stops a task
- `okube status` — shows cluster and task status
- `okube nodes` — lists worker nodes

## Multi-Service Deployment

### Manifest Format

Applications are defined in YAML manifests:

```yaml
name: my-app
services:
  db:
    image: postgres:15-alpine
    ports:
      "5432/tcp": "5432"
    env:
      POSTGRES_USER: demo
      POSTGRES_PASSWORD: demo123
      POSTGRES_DB: demodb
    volumes:
      - "/tmp/pgdata:/var/lib/postgresql/data"

  backend:
    image: my-backend:latest
    ports:
      "8080/tcp": "8080"
    env:
      PORT: "8080"
    dependsOn: [db]
    healthCheck: "/health"

  frontend:
    image: my-frontend:latest
    ports:
      "80/tcp": "3000"
    dependsOn: [backend]
```

### Deploy Flow

1. CLI sends the manifest to the manager (`POST /apps`)
2. Manager builds a dependency graph (AppGroup) and computes topological order
3. For each service in dependency order:
   a. **Inject discovery env vars** — e.g., the backend receives `DB_HOST=192.168.1.5` and `DB_PORT=5432`
   b. Create the task in etcd (state: Pending)
   c. Scheduler assigns it to a worker (state: Scheduled)
   d. Worker pulls the image and starts the container (state: Running)
   e. Manager waits until Running, then records the worker IP + mapped port
4. Final result returned to CLI with all service addresses

### Service Discovery

No DNS, no service mesh. Simple env-var injection:

- When service `backend` depends on service `db`, the backend container receives:
  - `DB_HOST=<ip-of-worker-running-db>`
  - `DB_PORT=<mapped-host-port-of-db>`
- The env var prefix is the uppercase service name with `-` and `.` replaced by `_`
- Services must read these env vars to connect to their dependencies

### Teardown

`okube delete my-app` stops services in **reverse** topological order (dependents first, then dependencies) and removes the app record from etcd.

## Task Lifecycle

```
Pending → Scheduled → Running → Completed
                  ↘           ↘
                  Failed ←───┘
                    ↓
                 Scheduled (restart, up to 3 times)
```

## Network Architecture (LAN)

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│  Laptop A    │     │  Laptop B    │     │  Laptop C    │
│  (Manager)   │     │  (Worker 1)  │     │  (Worker 2)  │
│              │     │              │     │              │
│  etcd        │     │  Docker      │     │  Docker      │
│  okube mgr   │◄───►│  okube wkr   │     │  okube wkr   │
│              │◄───►│              │     │              │
│              │     │  containers  │     │  containers  │
└──────┬───────┘     └──────┬───────┘     └──────┬───────┘
       │                    │                    │
       └────────────────────┴────────────────────┘
                    Same WiFi / LAN
                  (192.168.x.x network)
```

- Workers auto-detect their LAN IP and register it with the manager
- All communication is plain HTTP over the LAN
- Containers bind to host ports, accessible from any machine on the network
- No overlay network — containers on different nodes communicate via host IP:port

## Key Design Decisions

- **Host-port binding** instead of overlay networking (simple, sufficient for LAN)
- **Env-var service discovery** instead of DNS (no CoreDNS needed)
- **Sequential deployment** in dependency order (simpler than parallel with barriers)
- **Single replica per service** (no scaling, can be added later)
- **Docker API directly** (no CRI abstraction layer)
- **etcd for all state** (leader election, task state, app records, topology)
