# Okube Deployment Guide

Step-by-step guide to deploying a multi-service application across multiple laptops using okube.

---

## Table of Contents

1. [Prerequisites](#1-prerequisites)
2. [Network Setup](#2-network-setup)
3. [Install & Build Okube](#3-install--build-okube)
4. [Start etcd](#4-start-etcd)
5. [Start the Manager](#5-start-the-manager)
6. [Start Workers](#6-start-workers)
7. [Verify the Cluster](#7-verify-the-cluster)
8. [Write Your Manifest](#8-write-your-manifest)
9. [Build & Distribute Docker Images](#9-build--distribute-docker-images)
10. [Deploy Your Application](#10-deploy-your-application)
11. [Access Your Application](#11-access-your-application)
12. [Tear Down](#12-tear-down)
13. [Troubleshooting](#13-troubleshooting)

---

## 1. Prerequisites

Install these on **every laptop** that will participate in the cluster:

### Docker

```bash
# macOS
brew install --cask docker
# Then open Docker Desktop and wait for it to start

# Ubuntu/Debian
sudo apt-get update && sudo apt-get install -y docker.io
sudo systemctl start docker
sudo usermod -aG docker $USER
```

Verify: `docker run hello-world`

### Go (1.22+)

```bash
# macOS
brew install go

# Ubuntu
sudo apt-get install -y golang
```

Verify: `go version`

### etcd (manager laptop only)

```bash
# macOS
brew install etcd

# Ubuntu/Debian
ETCD_VER=v3.5.14
curl -L https://github.com/etcd-io/etcd/releases/download/${ETCD_VER}/etcd-${ETCD_VER}-linux-amd64.tar.gz -o etcd.tar.gz
tar xzf etcd.tar.gz
sudo mv etcd-${ETCD_VER}-linux-amd64/etcd* /usr/local/bin/
```

Verify: `etcd --version`

---

## 2. Network Setup

### Connect to the Same WiFi

All laptops must be on the **same WiFi network** (or wired LAN).

### Find Each Laptop's IP Address

```bash
# macOS
ipconfig getifaddr en0

# Linux
hostname -I | awk '{print $1}'
```

**Write down the IPs.** Example setup:

| Role    | Laptop | IP Address   |
| ------- | ------ | ------------ |
| Manager | A      | 192.168.1.10 |
| Worker  | B      | 192.168.1.11 |
| Worker  | C      | 192.168.1.12 |

### Verify Connectivity

From each laptop, ping every other laptop:

```bash
ping -c 3 192.168.1.10
ping -c 3 192.168.1.11
ping -c 3 192.168.1.12
```

### macOS Firewall

When you start okube or Docker for the first time, macOS will ask:

> "Do you want the application to accept incoming network connections?"

**Click "Allow"** for both `okube` and `com.docker.backend`.

If you already clicked "Deny", go to:
**System Settings → Network → Firewall → Options** and add exceptions.

---

## 3. Install & Build Okube

On **every laptop**:

```bash
# Clone the repo
git clone https://github.com/aditip149209/okube.git
cd okube

# Build the binary
go build -o okube .

# Verify
./okube --help
```

You should see the CLI help with commands like `manager`, `worker`, `deploy`, etc.

---

## 4. Start etcd

On the **manager laptop** (Laptop A) only:

```bash
etcd \
  --listen-client-urls http://0.0.0.0:2379 \
  --advertise-client-urls http://192.168.1.10:2379
```

Replace `192.168.1.10` with Laptop A's actual IP.

**Keep this terminal open.** etcd must stay running.

### Verify etcd

Open a new terminal on Laptop A:

```bash
etcdctl endpoint health --endpoints=http://localhost:2379
```

Expected output: `localhost:2379 is healthy`

---

## 5. Start the Manager

On the **manager laptop** (Laptop A), in a **new terminal**:

```bash
./okube manager \
  --host 0.0.0.0 \
  --port 5556 \
  --etcd-endpoints localhost:2379 \
  --scheduler epvm
```

**Keep this terminal open.** You should see:

```
Starting manager <hostname> on http://0.0.0.0:5556
```

The manager auto-detects leader election (it becomes leader since it's the only manager).

---

## 6. Start Workers

On **each worker laptop** (Laptops B, C), in a terminal:

```bash
./okube worker \
  --port 5556 \
  --manager-host 192.168.1.10 \
  --manager-port 5556
```

Replace `192.168.1.10` with the **manager laptop's IP**.

**Keep this terminal open.** You should see:

```
Starting worker.
Starting worker API on http://0.0.0.0:5556
```

The worker automatically:

1. Detects its own LAN IP (e.g., `192.168.1.11`)
2. Registers with the manager using that IP
3. Starts sending heartbeats every 10 seconds

### Optional: Override Advertised IP

If auto-detection picks the wrong interface:

```bash
./okube worker \
  --port 5556 \
  --manager-host 192.168.1.10 \
  --manager-port 5556 \
  --advertise-address 192.168.1.11
```

---

## 7. Verify the Cluster

From **any laptop** with the okube binary:

```bash
# Check manager status
./okube status --manager 192.168.1.10:5556

# List registered worker nodes
./okube nodes --manager 192.168.1.10:5556
```

Expected output for `nodes`:

```
ID                                    ADDRESS             HEARTBEAT
worker-abc123                         192.168.1.11:5556   2026-04-08T10:30:00Z
worker-def456                         192.168.1.12:5556   2026-04-08T10:30:05Z
```

If workers don't appear, check:

- Is the manager running? (`curl http://192.168.1.10:5556/status`)
- Can the worker reach the manager? (`curl http://192.168.1.10:5556/status` from the worker laptop)
- Is macOS firewall allowing connections?

---

## 8. Write Your Manifest

Create a `manifest.yaml` file describing your application. Here's a template:

```yaml
name: my-app
services:
  db:
    image: postgres:15-alpine
    ports:
      "5432/tcp": "5432"
    env:
      POSTGRES_USER: myuser
      POSTGRES_PASSWORD: mypass
      POSTGRES_DB: mydb
    volumes:
      - "/tmp/okube-pgdata:/var/lib/postgresql/data"

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

### Manifest Fields Reference

| Field                              | Description                                           |
| ---------------------------------- | ----------------------------------------------------- |
| `name`                             | Application name (used as app ID)                     |
| `services.<name>.image`            | Docker image name (must be available on worker nodes) |
| `services.<name>.ports`            | Port mappings: `"containerPort/tcp": "hostPort"`      |
| `services.<name>.env`              | Environment variables passed to the container         |
| `services.<name>.volumes`          | Host bind mounts: `"/host/path:/container/path"`      |
| `services.<name>.dependsOn`        | List of service names this service depends on         |
| `services.<name>.healthCheck`      | HTTP path for health checks (e.g., `/health`)         |
| `services.<name>.command`          | Override container entrypoint command                 |
| `services.<name>.resources.memory` | Memory request in MB                                  |
| `services.<name>.resources.disk`   | Disk request in MB                                    |

### Service Discovery Env Vars

When service `backend` has `dependsOn: [db]`, the backend container automatically receives:

- `DB_HOST=<IP of the worker running the db service>`
- `DB_PORT=<mapped host port of the db service>`

The env var prefix is the **uppercase service name** with `-` and `.` replaced by `_`.
Examples:

- Service `db` → `DB_HOST`, `DB_PORT`
- Service `my-cache` → `MY_CACHE_HOST`, `MY_CACHE_PORT`

**Your application code must read these env vars** to connect to its dependencies.

---

## 9. Build & Distribute Docker Images

Docker images must be available **on every worker node** that might run the service. There's no built-in image registry in okube.

### Option A: Build Locally on Each Worker

Copy your Dockerfiles to each worker laptop and build:

```bash
# On each worker laptop
cd my-backend/
docker build -t my-backend:latest .

cd my-frontend/
docker build -t my-frontend:latest .
```

### Option B: Use Docker Save/Load

Build once, transfer via USB or network:

```bash
# On the build machine
docker build -t my-backend:latest ./backend
docker save my-backend:latest | gzip > my-backend.tar.gz

# Copy to each worker, then:
docker load < my-backend.tar.gz
```

### Option C: Use Docker Hub or a Registry

```bash
# Push from build machine
docker build -t yourusername/my-backend:latest ./backend
docker push yourusername/my-backend:latest

# Workers will auto-pull from Docker Hub when the task starts
# Use the full image name in your manifest:
#   image: yourusername/my-backend:latest
```

**Note:** Public images like `postgres:15-alpine` are pulled automatically from Docker Hub. You only need to distribute custom images.

---

## 10. Deploy Your Application

From **any laptop** with the okube binary and your manifest:

```bash
./okube deploy -f manifest.yaml --manager 192.168.1.10:5556
```

You'll see output like:

```
Deploying application...

Application "my-app" deployed — status: running

SERVICE    ADDRESS              TASK ID                               WORKER
db         192.168.1.11:5432    a1b2c3d4-...                         192.168.1.11:5556
backend    192.168.1.12:8080    e5f6a7b8-...                         192.168.1.12:5556
frontend   192.168.1.11:3000    c9d0e1f2-...                         192.168.1.11:5556
```

The deploy process:

1. Starts `db` first (no dependencies)
2. Waits until `db` is Running
3. Starts `backend` with `DB_HOST=192.168.1.11` and `DB_PORT=5432` injected
4. Waits until `backend` is Running
5. Starts `frontend` with `BACKEND_HOST=192.168.1.12` and `BACKEND_PORT=8080` injected

### Check Status

```bash
# List deployed apps
./okube apps --manager 192.168.1.10:5556

# See all tasks
./okube status --manager 192.168.1.10:5556
```

---

## 11. Access Your Application

Open a browser on **any device on the same WiFi** and go to:

```
http://<FRONTEND_WORKER_IP>:<FRONTEND_PORT>
```

For example: `http://192.168.1.11:3000`

Test the backend directly:

```bash
curl http://192.168.1.12:8080/health
```

---

## 12. Tear Down

### Stop the Application

```bash
./okube delete my-app --manager 192.168.1.10:5556
```

This stops containers in reverse dependency order (frontend → backend → db).

### Stop Workers

Press `Ctrl+C` in each worker terminal.

### Stop Manager

Press `Ctrl+C` in the manager terminal.

### Stop etcd

Press `Ctrl+C` in the etcd terminal.

### Clean Up Docker

On each worker, if you want to remove stopped containers and images:

```bash
docker container prune -f
docker image prune -f
```

---

## 13. Troubleshooting

### "Connection refused" when starting worker

- **Cause**: Worker can't reach the manager.
- **Fix**: Verify the manager is running. Run `curl http://<MANAGER_IP>:5556/status` from the worker laptop.

### Worker not appearing in `okube nodes`

- **Cause**: Registration failed or heartbeat stale.
- **Fix**: Check the worker terminal for errors. Ensure `--manager-host` points to the correct IP. Check macOS firewall.

### "No workers with heartbeat" during deploy

- **Cause**: No healthy workers registered.
- **Fix**: Start at least one worker and wait 10 seconds for its heartbeat.

### Container won't start on a worker

- **Cause**: Docker image not available on that worker.
- **Fix**: Build or pull the image on the worker laptop: `docker pull <image>` or `docker build -t <image> .`
- **Debug**: Run `docker logs <container_id>` on the worker laptop (find the container ID from `okube status`).

### Service can't connect to its dependency

- **Cause**: Env vars not being read, or wrong port.
- **Fix**: `docker exec -it <container_id> env` on the worker to verify `DB_HOST`, `DB_PORT` etc. are set.
- Ensure your app reads the env vars at startup (not hardcoded connection strings).

### etcd connection failed

- **Cause**: etcd not running or wrong endpoint.
- **Fix**: Ensure etcd is running: `etcdctl endpoint health`. Check `--etcd-endpoints` flag matches where etcd is listening.

### Deploy hangs on a service

- **Cause**: Container is failing to start or health check is failing.
- **Fix**: Check the worker terminal for Docker errors. Check `docker ps -a` on workers to see if the container crashed. Check `docker logs <id>`.

### "app already exists" error

- **Cause**: You're re-deploying an app with the same name.
- **Fix**: Delete the old app first: `./okube delete my-app --manager <IP>:5556`, then deploy again.

### Laptop went to sleep / WiFi disconnected

- **Cause**: Heartbeats stop, manager considers worker dead.
- **Fix**: Wake the laptop, reconnect WiFi. The worker's heartbeat will resume. Running containers will still be there — the manager will re-discover them on the next sync cycle.

---

## Quick Reference

```bash
# === On Manager Laptop ===
# Terminal 1: etcd
etcd --listen-client-urls http://0.0.0.0:2379 --advertise-client-urls http://<MANAGER_IP>:2379

# Terminal 2: manager
./okube manager --host 0.0.0.0 --port 5556 --etcd-endpoints localhost:2379 --scheduler epvm

# === On Each Worker Laptop ===
./okube worker --port 5556 --manager-host <MANAGER_IP> --manager-port 5556

# === From Any Laptop ===
./okube nodes   --manager <MANAGER_IP>:5556           # list nodes
./okube deploy  -f manifest.yaml --manager <MANAGER_IP>:5556  # deploy app
./okube apps    --manager <MANAGER_IP>:5556           # list apps
./okube status  --manager <MANAGER_IP>:5556           # list tasks
./okube delete  my-app --manager <MANAGER_IP>:5556    # teardown app
```
