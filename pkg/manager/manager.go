package manager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/aditip149209/okube/pkg/manifest"
	"github.com/aditip149209/okube/pkg/node"
	"github.com/aditip149209/okube/pkg/scheduler"
	"github.com/aditip149209/okube/pkg/store"
	"github.com/aditip149209/okube/pkg/task"
	"github.com/aditip149209/okube/pkg/topology"
	"github.com/docker/go-connections/nat"
	"github.com/go-chi/chi"
	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

const heartbeatStaleAfter = 30 * time.Second
const defaultLeaderTTL = 15 * time.Second

type ManagerRole string

const (
	ManagerRoleLeader   ManagerRole = "leader"
	ManagerRoleFollower ManagerRole = "follower"
)

var ErrNotLeader = errors.New("manager: not leader")

type managerInfo struct {
	ID        string    `json:"id"`
	Address   string    `json:"address,omitempty"`
	Timestamp time.Time `json:"timestamp"`
}

func (m *Manager) buildManagerInfo() managerInfo {
	return managerInfo{ID: m.ID, Address: m.AdvertiseAddr, Timestamp: time.Now().UTC()}
}

func decodeManagerInfo(payload []byte) (*managerInfo, error) {
	if len(payload) == 0 {
		return nil, fmt.Errorf("empty manager info payload")
	}

	var info managerInfo
	if err := json.Unmarshal(payload, &info); err != nil {
		return nil, err
	}

	return &info, nil
}

type Config struct {
	Workers                 []string
	SchedulerType           string
	QueueSortStrategy       string
	TopologyProbeMode       string
	TopologyProbeInterval   time.Duration
	TopologyProbeSampleSize int
	Store                   store.Store
	Role                    ManagerRole
	ID                      string
	AdvertiseAddr           string
	WorkerClient            WorkerCommunicator
}

type Manager struct {
	ID                  string
	Role                ManagerRole
	AdvertiseAddr       string
	roleMu              sync.RWMutex
	Pending             queue.Queue
	Scheduler           scheduler.Scheduler
	WorkerClient        WorkerCommunicator
	Store               store.Store
	initialWorkers      []string
	etcdStore           *store.EtcdStore
	etcdSession         *concurrency.Session
	leaderKey           string
	managerKey          string
	electionStop        chan struct{}
	taskWatchStop       context.CancelFunc
	topologyUpdater     *topology.Updater
	topologyUpdaterStop context.CancelFunc
}

func (m *Manager) startLeaderElection() {
	go m.leaderElectionLoop()
}

func (m *Manager) leaderElectionLoop() {
	retryDelay := 2 * time.Second

	for {
		select {
		case <-m.electionStop:
			return
		default:
		}

		if m.etcdStore == nil {
			return
		}

		session, err := concurrency.NewSession(m.etcdStore.Client(), concurrency.WithTTL(int(defaultLeaderTTL/time.Second)))
		if err != nil {
			log.Printf("Manager %s: unable to create etcd session for leader election: %v", m.ID, err)
			time.Sleep(retryDelay)
			continue
		}

		m.etcdSession = session

		if err := m.registerManager(session); err != nil {
			log.Printf("Manager %s: failed to register manager presence: %v", m.ID, err)
			_ = session.Close()
			time.Sleep(retryDelay)
			continue
		}

		// Attempt to become the leader immediately on startup.
		m.tryAcquireLeadership(session)

		watchCtx, cancel := context.WithCancel(context.Background())
		go m.watchLeaderKey(watchCtx, session)

		<-session.Done()
		cancel()
		m.setRole(ManagerRoleFollower)
		log.Printf("Manager %s: etcd session closed; stepping down to follower", m.ID)
		time.Sleep(retryDelay)
	}
}

func (m *Manager) registerManager(session *concurrency.Session) error {
	if session == nil {
		return fmt.Errorf("etcd session is not initialized")
	}

	info := m.buildManagerInfo()
	payload, err := json.Marshal(info)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = m.etcdStore.Client().Put(ctx, m.managerKey, string(payload), clientv3.WithLease(session.Lease()))
	return err
}

func (m *Manager) tryAcquireLeadership(session *concurrency.Session) bool {
	if session == nil {
		return false
	}

	info := m.buildManagerInfo()
	payload, err := json.Marshal(info)
	if err != nil {
		log.Printf("Manager %s: failed to marshal leader payload: %v", m.ID, err)
		return false
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	txn := m.etcdStore.Client().Txn(ctx).If(
		clientv3.Compare(clientv3.CreateRevision(m.leaderKey), "=", 0),
	).Then(
		clientv3.OpPut(m.leaderKey, string(payload), clientv3.WithLease(session.Lease())),
	)

	resp, err := txn.Commit()
	if err != nil {
		log.Printf("Manager %s: leader election transaction failed: %v", m.ID, err)
		return false
	}

	if resp.Succeeded {
		m.onBecameLeader()
		return true
	}

	m.setRole(ManagerRoleFollower)
	return false
}

func (m *Manager) onBecameLeader() {
	wasLeader := m.IsLeader()
	m.setRole(ManagerRoleLeader)
	if !wasLeader {
		log.Printf("Manager %s: became leader", m.ID)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		m.registerWorkers(ctx, m.initialWorkers)
		cancel()
	}
}

func (m *Manager) watchLeaderKey(ctx context.Context, session *concurrency.Session) {
	if session == nil {
		return
	}

	client := m.etcdStore.Client()
	resp, err := client.Get(ctx, m.leaderKey)
	if err == nil {
		if resp == nil || resp.Count == 0 {
			m.tryAcquireLeadership(session)
		} else if len(resp.Kvs) > 0 {
			info, decodeErr := decodeManagerInfo(resp.Kvs[0].Value)
			if decodeErr != nil {
				log.Printf("Manager %s: failed to decode leader info: %v", m.ID, decodeErr)
			} else if info.ID == m.ID {
				m.setRole(ManagerRoleLeader)
			} else {
				m.setRole(ManagerRoleFollower)
			}
		}
	}

	watchOpts := []clientv3.OpOption{}
	if resp != nil {
		watchOpts = append(watchOpts, clientv3.WithRev(resp.Header.Revision+1))
	}

	watchChan := client.Watch(ctx, m.leaderKey, watchOpts...)
	for watchResp := range watchChan {
		if watchResp.Canceled {
			return
		}

		for _, ev := range watchResp.Events {
			switch ev.Type {
			case mvccpb.DELETE:
				log.Printf("Manager %s: detected missing leader key; attempting promotion", m.ID)
				m.setRole(ManagerRoleFollower)
				m.tryAcquireLeadership(session)
			case mvccpb.PUT:
				info, decodeErr := decodeManagerInfo(ev.Kv.Value)
				if decodeErr != nil {
					log.Printf("Manager %s: failed to decode leader info update: %v", m.ID, decodeErr)
					continue
				}

				if info.ID == m.ID {
					m.setRole(ManagerRoleLeader)
				} else {
					m.setRole(ManagerRoleFollower)
				}
			}
		}
	}
}

func (m *Manager) startTaskWatch() {
	if m.etcdStore == nil {
		return
	}

	// Always stop any existing watch before starting a new one to avoid
	// duplicate schedulers running after role changes.
	m.stopTaskWatch()

	ctx, cancel := context.WithCancel(context.Background())
	m.taskWatchStop = cancel

	go m.watchPendingTasks(ctx)
}

func (m *Manager) stopTaskWatch() {
	if m.taskWatchStop != nil {
		m.taskWatchStop()
		m.taskWatchStop = nil
	}
}

func (m *Manager) startTopologyUpdater() {
	if m.topologyUpdater == nil {
		return
	}

	m.stopTopologyUpdater()
	ctx, cancel := context.WithCancel(context.Background())
	m.topologyUpdaterStop = cancel
	go m.topologyUpdater.Run(ctx)
}

func (m *Manager) stopTopologyUpdater() {
	if m.topologyUpdaterStop != nil {
		m.topologyUpdaterStop()
		m.topologyUpdaterStop = nil
	}
}

func (m *Manager) listTopologyNodes(ctx context.Context) ([]topology.NodeTarget, error) {
	workers, err := m.activeWorkers(ctx)
	if err != nil {
		return nil, err
	}

	nodes := make([]topology.NodeTarget, 0, len(workers))
	for _, w := range workers {
		nodes = append(nodes, topology.NodeTarget{ID: w.ID, Address: w.Address})
	}
	return nodes, nil
}

func (m *Manager) probeLatency(ctx context.Context, from topology.NodeTarget, to topology.NodeTarget) (float64, error) {
	if from.ID == to.ID {
		return 0, nil
	}

	url := fmt.Sprintf("http://%s/stats", to.Address)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, err
	}

	start := time.Now()
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)

	if resp.StatusCode >= 400 {
		return 0, fmt.Errorf("probe failed with status %d", resp.StatusCode)
	}

	return float64(time.Since(start)) / float64(time.Millisecond), nil
}

func (m *Manager) watchPendingTasks(ctx context.Context) {
	// Catch up on any pending tasks that already exist before the watch starts.
	m.schedulePendingSnapshot()

	watchChan := m.etcdStore.Client().Watch(ctx, m.etcdStore.TasksPrefix(), clientv3.WithPrefix())
	for {
		select {
		case <-ctx.Done():
			return
		case watchResp, ok := <-watchChan:
			if !ok || watchResp.Canceled {
				return
			}

			for _, ev := range watchResp.Events {
				if ev.Type != mvccpb.PUT {
					continue
				}

				key := string(ev.Kv.Key)
				if !strings.HasSuffix(key, "/state") {
					continue
				}

				var state task.State
				if err := json.Unmarshal(ev.Kv.Value, &state); err != nil {
					log.Printf("Manager %s: failed to decode task state update for key %s: %v", m.ID, key, err)
					continue
				}

				if state != task.Pending {
					continue
				}

				taskID, err := parseTaskIDFromStateKey(key)
				if err != nil {
					log.Printf("Manager %s: unable to parse task ID from key %s: %v", m.ID, key, err)
					continue
				}

				_ = taskID // task id parse keeps key validation logic; snapshot handles ordering.
				go m.schedulePendingSnapshot()
			}
		}
	}
}

func parseTaskIDFromStateKey(key string) (uuid.UUID, error) {
	segments := strings.Split(strings.Trim(key, "/"), "/")
	for idx, segment := range segments {
		if segment == "tasks" && idx+1 < len(segments) {
			return uuid.Parse(segments[idx+1])
		}
	}
	return uuid.Nil, fmt.Errorf("could not parse task ID from key %s", key)
}

func (m *Manager) schedulePendingSnapshot() {
	if !m.IsLeader() || m.Store == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	records, err := m.Store.ListTasks(ctx)
	if err != nil {
		log.Printf("Manager %s: unable to list tasks for pending snapshot: %v", m.ID, err)
		return
	}

	appGroups, err := m.Store.ListAppGroups(ctx)
	if err != nil {
		log.Printf("Manager %s: unable to list appgroups for queue sorting: %v", m.ID, err)
		appGroups = nil
	}

	pendingTasks := make([]*task.Task, 0)

	for _, rec := range records {
		if rec.Task != nil && rec.Task.State == task.Pending {
			pendingTasks = append(pendingTasks, rec.Task)
		}
	}

	if len(pendingTasks) == 0 {
		return
	}

	sorted := pendingTasks
	if qs, ok := m.Scheduler.(scheduler.QueueSortCapable); ok {
		sorted = qs.QueueSort(pendingTasks, appGroups)
	}

	for _, t := range sorted {
		if t == nil {
			continue
		}
		m.trySchedulePendingTask(t.ID)
	}
}

func (m *Manager) trySchedulePendingTask(taskID uuid.UUID) {
	if taskID == uuid.Nil {
		return
	}

	if !m.IsLeader() || m.Store == nil || m.etcdStore == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t, _, err := m.Store.GetTask(ctx, taskID)
	cancel()
	if err != nil {
		if !errors.Is(err, store.ErrNotFound) {
			log.Printf("Manager %s: failed to fetch pending task %s: %v", m.ID, taskID, err)
		}
		return
	}

	if t.State != task.Pending {
		return
	}

	workerCtx, workerCancel := context.WithTimeout(context.Background(), 5*time.Second)
	worker, err := m.SelectWorker(workerCtx, *t)
	workerCancel()
	if err != nil {
		log.Printf("Manager %s: no worker selected for task %s: %v", m.ID, t.ID, err)
		return
	}

	assignCtx, assignCancel := context.WithTimeout(context.Background(), 5*time.Second)
	succeeded, err := m.etcdStore.AssignPendingTask(assignCtx, t, worker.ID)
	assignCancel()
	if err != nil {
		log.Printf("Manager %s: failed to assign task %s to worker %s: %v", m.ID, t.ID, worker.ID, err)
		return
	}

	if !succeeded {
		log.Printf("Manager %s: task %s was already scheduled by another manager", m.ID, t.ID)
		return
	}

	reserveCtx, reserveCancel := context.WithTimeout(context.Background(), 5*time.Second)
	err = m.reserveBandwidthForTaskPlacement(reserveCtx, *t, worker.ID)
	reserveCancel()
	if err != nil {
		log.Printf("Manager %s: failed bandwidth reservation for task %s on worker %s: %v", m.ID, t.ID, worker.ID, err)
		m.resetTaskToPending(*t)
		return
	}

	t.State = task.Scheduled
	m.dispatchTaskToWorker(*t, worker)
}

func (m *Manager) dispatchTaskToWorker(t task.Task, worker *store.Worker) {
	if worker == nil {
		return
	}

	te := task.TaskEvent{
		ID:        uuid.New(),
		State:     task.Scheduled,
		Timestamp: time.Now().UTC(),
		Task:      t,
	}

	_, errResp, err := m.WorkerClient.StartTask(worker.Address, te)
	if err != nil {
		log.Printf("Manager %s: dispatch to worker %s for task %s failed: %v", m.ID, worker.ID, t.ID, err)
		releaseCtx, releaseCancel := context.WithTimeout(context.Background(), 5*time.Second)
		if releaseErr := m.releaseBandwidthForTaskPlacement(releaseCtx, t, worker.ID); releaseErr != nil {
			log.Printf("Manager %s: failed to release bandwidth for task %s after dispatch error: %v", m.ID, t.ID, releaseErr)
		}
		releaseCancel()
		m.resetTaskToPending(t)
		return
	}

	if errResp != nil {
		log.Printf("Manager %s: worker %s rejected task %s: %s", m.ID, worker.ID, t.ID, errResp.Message)
		releaseCtx, releaseCancel := context.WithTimeout(context.Background(), 5*time.Second)
		if releaseErr := m.releaseBandwidthForTaskPlacement(releaseCtx, t, worker.ID); releaseErr != nil {
			log.Printf("Manager %s: failed to release bandwidth for task %s after worker rejection: %v", m.ID, t.ID, releaseErr)
		}
		releaseCancel()
		m.resetTaskToPending(t)
		return
	}

	// Mark task as running after the worker acknowledged receipt.
	t.State = task.Running
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := m.Store.UpdateTaskState(ctx, &t, worker.ID); err != nil {
		log.Printf("Manager %s: failed to mark task %s running: %v", m.ID, t.ID, err)
	}
	cancel()
}

func (m *Manager) resetTaskToPending(t task.Task) {
	if m.Store == nil {
		return
	}

	t.State = task.Pending
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := m.Store.UpdateTaskState(ctx, &t, ""); err != nil {
		log.Printf("Manager %s: failed to revert task %s to pending: %v", m.ID, t.ID, err)
	}
	cancel()
}

func (m *Manager) activeWorkers(ctx context.Context) ([]store.Worker, error) {
	if m.Store == nil {
		return nil, errors.New("store not configured")
	}

	workers, err := m.Store.ListWorkers(ctx)
	if err != nil {
		return nil, err
	}

	cutoff := time.Now().UTC().Add(-heartbeatStaleAfter)
	live := make([]store.Worker, 0, len(workers))
	for _, w := range workers {
		if w.Heartbeat.After(cutoff) {
			live = append(live, w)
		}
	}

	if len(live) == 0 {
		return nil, fmt.Errorf("no workers with heartbeat in the last %s", heartbeatStaleAfter)
	}

	return live, nil
}

func (m *Manager) SelectWorker(ctx context.Context, t task.Task) (*store.Worker, error) {
	if m.Store == nil {
		return nil, errors.New("store not configured")
	}

	workers, err := m.activeWorkers(ctx)
	if err != nil {
		return nil, err
	}

	workerMap := make(map[string]store.Worker)
	nodes := make([]*node.Node, 0, len(workers))
	for _, w := range workers {
		workerMap[w.ID] = w
		n := node.NewNode(w.ID, w.Address, "worker")
		nodes = append(nodes, n)
	}

	filterCtx := m.buildFilterContext(ctx, t)
	candidates := m.Scheduler.SelectCandidateNodes(t, nodes, filterCtx)
	if len(candidates) == 0 {
		return nil, fmt.Errorf("no available candidates match resource request for task %v", t.ID)
	}

	scoreCtx := &scheduler.ScoreContext{
		Filter:         filterCtx,
		NetworkWeight:  0.7,
		ResourceWeight: 0.3,
	}
	scores := m.Scheduler.Score(t, candidates, scoreCtx)
	selectedNode := m.Scheduler.Pick(scores, candidates)
	if selectedNode == nil {
		return nil, fmt.Errorf("scheduler failed to pick a worker for task %v", t.ID)
	}

	selectedWorker, ok := workerMap[selectedNode.Name]
	if !ok {
		return nil, fmt.Errorf("selected worker %s not found", selectedNode.Name)
	}

	return &selectedWorker, nil

}

func (m *Manager) buildFilterContext(ctx context.Context, t task.Task) *scheduler.FilterContext {
	if m.Store == nil || t.AppID == "" {
		return nil
	}

	serviceID := taskServiceID(&t)
	if serviceID == "" {
		return nil
	}

	ag, err := m.Store.GetAppGroup(ctx, t.AppID)
	if err != nil {
		if !errors.Is(err, store.ErrNotFound) {
			log.Printf("Manager %s: failed to load appgroup %s for filter context: %v", m.ID, t.AppID, err)
		}
		return nil
	}

	topo, err := m.Store.GetNetworkTopology(ctx)
	if err != nil {
		if !errors.Is(err, store.ErrNotFound) {
			log.Printf("Manager %s: failed to load network topology for filter context: %v", m.ID, err)
		}
		return nil
	}

	records, err := m.Store.ListTasks(ctx)
	if err != nil {
		log.Printf("Manager %s: failed to list tasks for filter context: %v", m.ID, err)
		return nil
	}

	dependencyNodeByService := make(map[string]string)
	for _, rec := range records {
		if rec.Task == nil || rec.Task.AppID != t.AppID {
			continue
		}
		if rec.Task.State != task.Running && rec.Task.State != task.Scheduled {
			continue
		}

		sid := taskServiceID(rec.Task)
		if sid == "" {
			continue
		}

		nodeID, err := m.Store.GetNodeOfTask(ctx, rec.Task.ID)
		if err != nil {
			if !errors.Is(err, store.ErrNotFound) {
				log.Printf("Manager %s: failed to read node assignment for task %s: %v", m.ID, rec.Task.ID, err)
			}
			continue
		}
		dependencyNodeByService[sid] = nodeID
	}

	return &scheduler.FilterContext{
		AppGroup:                ag,
		NetworkTopology:         topo,
		DependencyNodeByService: dependencyNodeByService,
	}
}

func taskServiceID(t *task.Task) string {
	if t == nil {
		return ""
	}
	if t.ServiceID != "" {
		return t.ServiceID
	}
	return t.Name
}

func (m *Manager) reserveBandwidthForTaskPlacement(ctx context.Context, t task.Task, candidateNodeID string) error {
	if m.Store == nil || t.AppID == "" {
		return nil
	}

	filterCtx := m.buildFilterContext(ctx, t)
	if filterCtx == nil || filterCtx.AppGroup == nil || filterCtx.NetworkTopology == nil {
		return nil
	}

	serviceID := taskServiceID(&t)
	if serviceID == "" {
		return nil
	}

	edges := filterCtx.AppGroup.GetDependencies(serviceID)
	type reservation struct {
		nodeA  string
		nodeB  string
		amount float64
	}
	reserved := make([]reservation, 0)

	for _, edge := range edges {
		if edge.MinBandwidth == nil || *edge.MinBandwidth <= 0 {
			continue
		}

		depNodeID, ok := filterCtx.DependencyNodeByService[edge.To]
		if !ok || depNodeID == "" || depNodeID == candidateNodeID {
			continue
		}

		if ok := filterCtx.NetworkTopology.ReserveBandwidth(candidateNodeID, depNodeID, *edge.MinBandwidth); !ok {
			for _, r := range reserved {
				filterCtx.NetworkTopology.ReleaseBandwidth(r.nodeA, r.nodeB, r.amount)
			}
			return fmt.Errorf("insufficient available bandwidth from %s to %s", candidateNodeID, depNodeID)
		}

		reserved = append(reserved, reservation{nodeA: candidateNodeID, nodeB: depNodeID, amount: *edge.MinBandwidth})
	}

	if len(reserved) == 0 {
		return nil
	}

	return m.Store.SaveNetworkTopology(ctx, filterCtx.NetworkTopology)
}

func (m *Manager) releaseBandwidthForTaskPlacement(ctx context.Context, t task.Task, candidateNodeID string) error {
	if m.Store == nil || t.AppID == "" {
		return nil
	}

	filterCtx := m.buildFilterContext(ctx, t)
	if filterCtx == nil || filterCtx.AppGroup == nil || filterCtx.NetworkTopology == nil {
		return nil
	}

	serviceID := taskServiceID(&t)
	if serviceID == "" {
		return nil
	}

	edges := filterCtx.AppGroup.GetDependencies(serviceID)
	releasedAny := false
	for _, edge := range edges {
		if edge.MinBandwidth == nil || *edge.MinBandwidth <= 0 {
			continue
		}

		depNodeID, ok := filterCtx.DependencyNodeByService[edge.To]
		if !ok || depNodeID == "" || depNodeID == candidateNodeID {
			continue
		}

		if filterCtx.NetworkTopology.ReleaseBandwidth(candidateNodeID, depNodeID, *edge.MinBandwidth) {
			releasedAny = true
		}
	}

	if !releasedAny {
		return nil
	}

	return m.Store.SaveNetworkTopology(ctx, filterCtx.NetworkTopology)
}

func (m *Manager) updateTasks() {
	if !m.IsLeader() {
		log.Printf("Manager %s is in follower role; skipping task state updates", m.ID)
		return
	}
	if m.Store == nil {
		log.Println("Store not configured; skipping task update")
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	workers, err := m.activeWorkers(ctx)
	cancel()
	if err != nil {
		log.Printf("Error listing workers: %v", err)
		return
	}

	for _, worker := range workers {
		log.Printf("Checking worker %v for task updates", worker.ID)
		tasks, err := m.WorkerClient.FetchTasks(worker.Address)
		if err != nil {
			log.Printf("Error connecting to %v: %v", worker.Address, err)
			continue
		}

		for _, t := range tasks {
			log.Printf("Attempting to update task %v\n", t)

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			persisted, _, err := m.Store.GetTask(ctx, t.ID)
			cancel()
			if err != nil {
				if errors.Is(err, store.ErrNotFound) {
					log.Printf("Task with ID %s not found in store\n", t.ID)
					continue
				}
				log.Printf("Error retrieving task %s from store: %v", t.ID, err)
				continue
			}

			persisted.State = t.State
			persisted.StartTime = t.StartTime
			persisted.EndTime = t.EndTime
			persisted.ContainerID = t.ContainerID

			ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
			if err := m.Store.UpdateTaskState(ctx, persisted, worker.ID); err != nil {
				log.Printf("Error updating task %s in store: %v", t.ID, err)
			}
			cancel()
		}
	}

}

func (m *Manager) SendWork() {
	if !m.IsLeader() {
		log.Printf("Manager %s is in follower role; skipping scheduling", m.ID)
		return
	}
	if m.Pending.Len() == 0 {
		log.Println("No work in the queue")
		return
	}

	e := m.Pending.Dequeue()
	te, ok := e.(task.TaskEvent)
	if !ok {
		log.Printf("Unexpected item in queue: %T", e)
		return
	}

	log.Printf("Pulled task %v off the managers queue", te)

	if m.Store == nil {
		log.Println("Store not configured; cannot process task")
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	existingTask, existingWorker, existingErr := m.Store.GetTask(ctx, te.Task.ID)
	cancel()
	if existingErr != nil && !errors.Is(existingErr, store.ErrNotFound) {
		log.Printf("Error retrieving task %s from store: %v", te.Task.ID, existingErr)
		m.Pending.Enqueue(te)
		return
	}

	if existingWorker != "" && existingTask != nil {
		if te.State == task.Completed && task.ValidStateTransition(existingTask.State, te.State) {
			m.stopTask(existingWorker, te.Task.ID.String())
			return
		}

		log.Printf("Invalid request: existing task %s is in state %v and cannot transition to the completed state\n",
			existingTask.ID.String(), existingTask.State)
		return
	}

	t := te.Task
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	w, err := m.SelectWorker(ctx, t)
	cancel()

	if err != nil {
		log.Printf("Error selecting worker for task %v: %v", t, err)
		m.Pending.Enqueue(te)
		return

	}

	newTask, errResp, err := m.WorkerClient.StartTask(w.Address, te)
	if err != nil {
		log.Printf("Error connecting to %v: %v\n", w.ID, err)
		m.Pending.Enqueue(te)
		return
	}

	if errResp != nil {
		log.Printf("Response error (%d): %s", errResp.HTTPStatusCode, errResp.Message)
		return
	}

	if newTask != nil {
		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
		if errors.Is(existingErr, store.ErrNotFound) {
			if err := m.Store.CreateTask(ctx, newTask, w.ID); err != nil {
				log.Printf("Error persisting task %s: %v", newTask.ID, err)
			}
		} else {
			if err := m.Store.UpdateTaskState(ctx, newTask, w.ID); err != nil {
				log.Printf("Error updating task %s: %v", newTask.ID, err)
			}
		}
		cancel()
		log.Printf("%#v\n", *newTask)
	}
}

func (m *Manager) AddTask(te task.TaskEvent) error {
	if !m.IsLeader() {
		log.Printf("Manager %s is in follower role; rejecting task add", m.ID)
		return ErrNotLeader
	}

	if m.Store == nil {
		return errors.New("store not configured")
	}

	// Stop requests are handled directly against the assigned worker.
	if te.Task.State == task.Completed {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		_, workerID, err := m.Store.GetTask(ctx, te.Task.ID)
		if err != nil {
			return err
		}

		if workerID == "" {
			return fmt.Errorf("no worker assignment found for task %s", te.Task.ID)
		}

		m.stopTask(workerID, te.Task.ID.String())
		return nil
	}

	if te.Task.ID == uuid.Nil {
		te.Task.ID = uuid.New()
	}

	te.Task.State = task.Pending
	te.Timestamp = time.Now().UTC()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Persist immediately so a manager restart can restore tasks even before dispatch.
	return m.Store.CreateTask(ctx, &te.Task, "")
}

func New(workers []string, schedulerType string, st store.Store) *Manager {
	return NewWithConfig(Config{Workers: workers, SchedulerType: schedulerType, Store: st})
}

func NewWithConfig(cfg Config) *Manager {
	role := cfg.Role
	if role == "" {
		role = ManagerRoleFollower
	}

	id := cfg.ID
	if id == "" {
		id = defaultManagerID()
	}

	advertiseAddr := cfg.AdvertiseAddr
	if advertiseAddr == "" {
		advertiseAddr = os.Getenv("OKUBE_MANAGER_ADDRESS")
	}
	if advertiseAddr == "" {
		host := os.Getenv("CUBE_MANAGER_HOST")
		port := os.Getenv("CUBE_MANAGER_PORT")
		if host != "" && port != "" {
			advertiseAddr = fmt.Sprintf("%s:%s", host, port)
		}
	}

	s := scheduler.NewPipelineScheduler(cfg.SchedulerType, cfg.QueueSortStrategy)
	probeMode := topology.ParseProbeMode(cfg.TopologyProbeMode)
	probeInterval := cfg.TopologyProbeInterval
	if probeInterval <= 0 {
		probeInterval = 30 * time.Second
	}
	probeSampleSize := cfg.TopologyProbeSampleSize
	if probeSampleSize <= 0 {
		probeSampleSize = 2
	}

	wc := cfg.WorkerClient
	if wc == nil {
		wc = NewHTTPWorkerClient(nil)
	}

	initialWorkers := append([]string(nil), cfg.Workers...)

	m := &Manager{
		ID:             id,
		Pending:        *queue.New(),
		Scheduler:      s,
		WorkerClient:   wc,
		Store:          cfg.Store,
		AdvertiseAddr:  advertiseAddr,
		initialWorkers: initialWorkers,
		electionStop:   make(chan struct{}),
	}

	if m.Store != nil {
		m.topologyUpdater = topology.NewUpdater(topology.UpdaterConfig{
			Store:      m.Store,
			ListNodes:  m.listTopologyNodes,
			Probe:      m.probeLatency,
			Mode:       probeMode,
			Interval:   probeInterval,
			SampleSize: probeSampleSize,
		})
	}

	if etcdStore, ok := cfg.Store.(*store.EtcdStore); ok {
		m.etcdStore = etcdStore
		m.leaderKey = etcdStore.LeaderKey()
		m.managerKey = etcdStore.ManagerKey(m.ID)
	}

	m.setRole(role)

	if m.etcdStore != nil {
		m.startLeaderElection()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	m.registerWorkers(ctx, cfg.Workers)

	return m
}

func defaultManagerID() string {
	if envID := os.Getenv("OKUBE_MANAGER_ID"); envID != "" {
		return envID
	}

	if host, err := os.Hostname(); err == nil && host != "" {
		return host
	}

	return uuid.New().String()
}

func (m *Manager) IsLeader() bool {
	m.roleMu.RLock()
	defer m.roleMu.RUnlock()
	return m.Role == ManagerRoleLeader
}

// CurrentRole returns the manager's current role in a concurrency-safe way.
func (m *Manager) CurrentRole() ManagerRole {
	m.roleMu.RLock()
	defer m.roleMu.RUnlock()
	return m.Role
}

func (m *Manager) setRole(role ManagerRole) {
	m.roleMu.Lock()
	prev := m.Role
	m.Role = role
	m.roleMu.Unlock()

	if prev == role {
		return
	}

	if role == ManagerRoleLeader {
		m.startTaskWatch()
		m.startTopologyUpdater()
	} else {
		m.stopTaskWatch()
		m.stopTopologyUpdater()
	}
}

// LeaderAddress returns the advertised address of the current leader.
// When called on the leader, it returns the local advertise address.
// On a follower, it queries etcd for the leader metadata.
func (m *Manager) LeaderAddress(ctx context.Context) (string, error) {
	if m.IsLeader() {
		if m.AdvertiseAddr == "" {
			return "", fmt.Errorf("leader advertise address not configured")
		}
		return m.AdvertiseAddr, nil
	}

	if m.etcdStore == nil {
		return "", fmt.Errorf("etcd store not configured; cannot resolve leader")
	}

	lookupCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	resp, err := m.etcdStore.Client().Get(lookupCtx, m.leaderKey)
	if err != nil {
		return "", err
	}

	if resp.Count == 0 || len(resp.Kvs) == 0 {
		return "", fmt.Errorf("no leader present")
	}

	info, err := decodeManagerInfo(resp.Kvs[0].Value)
	if err != nil {
		return "", err
	}

	if info.Address != "" {
		return info.Address, nil
	}

	// Fallback: query the specific manager key for richer metadata if present.
	managerKey := m.etcdStore.ManagerKey(info.ID)
	managerResp, err := m.etcdStore.Client().Get(lookupCtx, managerKey)
	if err != nil {
		return "", err
	}

	if managerResp.Count == 0 || len(managerResp.Kvs) == 0 {
		return "", fmt.Errorf("leader metadata missing for %s", info.ID)
	}

	managerInfo, err := decodeManagerInfo(managerResp.Kvs[0].Value)
	if err != nil {
		return "", err
	}

	if managerInfo.Address == "" {
		return "", fmt.Errorf("leader address not published")
	}

	return managerInfo.Address, nil
}

func (m *Manager) registerWorkers(ctx context.Context, workers []string) {
	if m.Store == nil {
		return
	}

	if !m.IsLeader() {
		log.Printf("Manager %s is in follower role; skipping worker registration", m.ID)
		return
	}

	for _, worker := range workers {
		meta := store.Worker{ID: worker, Address: worker, Heartbeat: time.Now().UTC()}
		if err := m.Store.RegisterWorker(ctx, meta); err != nil {
			log.Printf("Error registering worker %s: %v", worker, err)
		}
	}
}

// manager api- this is what allows the users to interact with the okube cluster. essentially, we are building a way for the end user to interact with okube
type Api struct {
	Address    string
	Port       int
	Manager    *Manager
	Router     *chi.Mux
	HTTPClient *http.Client
}

type ErrResponse struct {
	HTTPStatusCode int    `json:"status"`
	Message        string `json:"message"`
}

func (a *Api) httpClient() *http.Client {
	if a.HTTPClient != nil {
		return a.HTTPClient
	}
	return http.DefaultClient
}

// forwardToLeader proxies the request to the current leader when this manager
// is a follower. It returns true if the request was handled (proxied or a
// redirect/error was sent), and false if the caller should continue local
// processing.
func (a *Api) forwardToLeader(w http.ResponseWriter, r *http.Request) bool {
	if a.Manager == nil || a.Manager.IsLeader() {
		return false
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	leaderAddr, err := a.Manager.LeaderAddress(ctx)
	if err != nil || leaderAddr == "" {
		msg := "leader unavailable; cannot forward request"
		if err != nil {
			msg = fmt.Sprintf("leader unavailable: %v", err)
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(ErrResponse{HTTPStatusCode: http.StatusServiceUnavailable, Message: msg})
		return true
	}

	targetURL := fmt.Sprintf("http://%s%s", leaderAddr, r.URL.RequestURI())
	req, err := http.NewRequestWithContext(ctx, r.Method, targetURL, r.Body)
	if err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(ErrResponse{HTTPStatusCode: http.StatusServiceUnavailable, Message: "failed to build forward request"})
		return true
	}

	req.Header = r.Header.Clone()

	resp, err := a.httpClient().Do(req)
	if err != nil {
		// If proxying fails, fall back to an HTTP redirect so the client can retry directly.
		http.Redirect(w, r, targetURL, http.StatusTemporaryRedirect)
		return true
	}
	defer resp.Body.Close()

	for k, values := range resp.Header {
		for _, v := range values {
			w.Header().Add(k, v)
		}
	}

	w.WriteHeader(resp.StatusCode)
	if _, copyErr := io.Copy(w, resp.Body); copyErr != nil {
		log.Printf("Manager %s: failed to copy proxied response: %v", a.Manager.ID, copyErr)
	}

	return true
}

func (a *Api) RegisterWorkerHandler(w http.ResponseWriter, r *http.Request) {
	if a.forwardToLeader(w, r) {
		return
	}
	if !a.Manager.IsLeader() {
		msg := "manager is follower; worker registration disabled"
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(ErrResponse{HTTPStatusCode: http.StatusServiceUnavailable, Message: msg})
		return
	}
	if a.Manager.Store == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	var worker store.Worker
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&worker); err != nil {
		msg := fmt.Sprintf("Error unmarshalling body: %v", err)
		log.Print(msg)
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrResponse{HTTPStatusCode: http.StatusBadRequest, Message: msg})
		return
	}

	if worker.ID == "" || worker.Address == "" {
		msg := "worker id and address are required"
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrResponse{HTTPStatusCode: http.StatusBadRequest, Message: msg})
		return
	}

	worker.Heartbeat = time.Now().UTC()
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	if err := a.Manager.Store.RegisterWorker(ctx, worker); err != nil {
		msg := fmt.Sprintf("Error registering worker %s: %v", worker.ID, err)
		log.Print(msg)
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(ErrResponse{HTTPStatusCode: http.StatusInternalServerError, Message: msg})
		return
	}

	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(worker)
}

func (a *Api) HeartbeatHandler(w http.ResponseWriter, r *http.Request) {
	if a.forwardToLeader(w, r) {
		return
	}
	if !a.Manager.IsLeader() {
		msg := "manager is follower; heartbeat updates disabled"
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(ErrResponse{HTTPStatusCode: http.StatusServiceUnavailable, Message: msg})
		return
	}
	if a.Manager.Store == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	workerID := chi.URLParam(r, "workerID")
	if workerID == "" {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrResponse{HTTPStatusCode: http.StatusBadRequest, Message: "worker id is required"})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	if err := a.Manager.Store.UpdateWorkerHeartbeat(ctx, workerID, time.Now().UTC()); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(ErrResponse{HTTPStatusCode: http.StatusNotFound, Message: "worker not registered"})
			return
		}
		msg := fmt.Sprintf("Error updating heartbeat for worker %s: %v", workerID, err)
		log.Print(msg)
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(ErrResponse{HTTPStatusCode: http.StatusInternalServerError, Message: msg})
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (a *Api) StartTaskHandler(w http.ResponseWriter, r *http.Request) {
	if a.forwardToLeader(w, r) {
		return
	}
	if !a.Manager.IsLeader() {
		msg := "manager is follower; task creation disabled"
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(ErrResponse{HTTPStatusCode: http.StatusServiceUnavailable, Message: msg})
		return
	}
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()

	te := task.TaskEvent{}

	err := d.Decode(&te)

	if err != nil {
		msg := fmt.Sprintf("Error unmarshalling body: %v\n", err)
		log.Print(msg)
		w.WriteHeader(400)
		e := ErrResponse{
			HTTPStatusCode: 400,
			Message:        msg,
		}
		json.NewEncoder(w).Encode(e)
		return
	}

	if err := a.Manager.AddTask(te); err != nil {
		msg := fmt.Sprintf("Unable to add task: %v", err)
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(ErrResponse{HTTPStatusCode: http.StatusServiceUnavailable, Message: msg})
		return
	}

	log.Printf("Added task %v\n", te.Task.ID)
	w.WriteHeader(201)
	json.NewEncoder(w).Encode(te)
}

func (a *Api) GetTasksHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	json.NewEncoder(w).Encode(a.Manager.GetTasks())
}

func (a *Api) StopTaskHandler(w http.ResponseWriter, r *http.Request) {
	if a.forwardToLeader(w, r) {
		return
	}
	if !a.Manager.IsLeader() {
		msg := "manager is follower; task stop disabled"
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(ErrResponse{HTTPStatusCode: http.StatusServiceUnavailable, Message: msg})
		return
	}
	taskID := chi.URLParam(r, "taskID")
	if taskID == "" {
		log.Printf("No TaskID passed in request.\n")
		w.WriteHeader(400)
	}

	tID, _ := uuid.Parse(taskID)
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	taskToStop, workerID, err := a.Manager.Store.GetTask(ctx, tID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			log.Printf("No task with ID %v found", tID)
			w.WriteHeader(404)
			return
		}
		log.Printf("Error retrieving task %v: %v", tID, err)
		w.WriteHeader(500)
		return
	}

	te := task.TaskEvent{
		ID:        uuid.New(),
		State:     task.Completed,
		Timestamp: time.Now(),
	}

	taskCopy := *taskToStop
	taskCopy.State = task.Completed
	te.Task = taskCopy
	te.Task.RestartCount = taskToStop.RestartCount
	// Preserve the worker assignment so it can be used during stop processing.
	if workerID != "" {
		if err := a.Manager.Store.CreateTask(ctx, &te.Task, workerID); err != nil {
			log.Printf("Error persisting stop request for task %v: %v", tID, err)
		}
	}
	if err := a.Manager.AddTask(te); err != nil {
		msg := fmt.Sprintf("Unable to enqueue stop request: %v", err)
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(ErrResponse{HTTPStatusCode: http.StatusServiceUnavailable, Message: msg})
		return
	}

	log.Printf("Added task event %v to stop task %v\n", te.ID, taskToStop.ID)
	w.WriteHeader(204)

}

func (m *Manager) GetTasks() []*task.Task {
	if m.Store == nil {
		return []*task.Task{}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	records, err := m.Store.ListTasks(ctx)
	if err != nil {
		log.Printf("Error retrieving tasks from store: %v", err)
		return []*task.Task{}
	}

	tasks := make([]*task.Task, 0, len(records))
	for _, rec := range records {
		tasks = append(tasks, rec.Task)
	}
	return tasks
}

// StatusHandler returns the current manager's role and the leader address so
// that clients can discover the cluster topology from any node.
func (a *Api) StatusHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	type statusResponse struct {
		ManagerID     string `json:"manager_id"`
		Role          string `json:"role"`
		LeaderAddress string `json:"leader_address,omitempty"`
	}

	resp := statusResponse{
		ManagerID: a.Manager.ID,
		Role:      string(a.Manager.CurrentRole()),
	}

	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	if addr, err := a.Manager.LeaderAddress(ctx); err == nil {
		resp.LeaderAddress = addr
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(resp)
}

// GetNodesHandler returns all registered workers (nodes) and their heartbeat
// status. This endpoint is readable from any manager (leader or follower)
// because it only performs read-only queries against the shared etcd store.
func (a *Api) GetNodesHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if a.Manager.Store == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(ErrResponse{HTTPStatusCode: http.StatusServiceUnavailable, Message: "store not configured"})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	workers, err := a.Manager.Store.ListWorkers(ctx)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(ErrResponse{HTTPStatusCode: http.StatusInternalServerError, Message: fmt.Sprintf("error listing workers: %v", err)})
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(workers)
}

// ---------------------------------------------------------------------------
// App deploy / list / get / delete handlers
// ---------------------------------------------------------------------------

// DeployAppHandler handles POST /apps — deploys a multi-service application
// from a manifest definition. Services are deployed in topological order with
// env-var-based service discovery.
func (a *Api) DeployAppHandler(w http.ResponseWriter, r *http.Request) {
	if a.forwardToLeader(w, r) {
		return
	}
	if !a.Manager.IsLeader() {
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(ErrResponse{HTTPStatusCode: http.StatusServiceUnavailable, Message: "not leader"})
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrResponse{HTTPStatusCode: http.StatusBadRequest, Message: "failed to read body"})
		return
	}

	m, err := manifest.ParseManifest(body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrResponse{HTTPStatusCode: http.StatusBadRequest, Message: fmt.Sprintf("invalid manifest: %v", err)})
		return
	}

	result, err := a.Manager.DeployApp(r.Context(), m)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(ErrResponse{HTTPStatusCode: http.StatusInternalServerError, Message: fmt.Sprintf("deploy failed: %v", err)})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(result)
}

// ListAppsHandler handles GET /apps.
func (a *Api) ListAppsHandler(w http.ResponseWriter, r *http.Request) {
	if a.Manager.Store == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	apps, err := a.Manager.Store.ListApps(ctx)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(ErrResponse{HTTPStatusCode: http.StatusInternalServerError, Message: err.Error()})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(apps)
}

// GetAppHandler handles GET /apps/{appName}.
func (a *Api) GetAppHandler(w http.ResponseWriter, r *http.Request) {
	if a.Manager.Store == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	appName := chi.URLParam(r, "appName")
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	app, err := a.Manager.Store.GetApp(ctx, appName)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(ErrResponse{HTTPStatusCode: 404, Message: "app not found"})
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(ErrResponse{HTTPStatusCode: 500, Message: err.Error()})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(app)
}

// DeleteAppHandler handles DELETE /apps/{appName}.
func (a *Api) DeleteAppHandler(w http.ResponseWriter, r *http.Request) {
	if a.forwardToLeader(w, r) {
		return
	}
	if !a.Manager.IsLeader() {
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(ErrResponse{HTTPStatusCode: http.StatusServiceUnavailable, Message: "not leader"})
		return
	}

	appName := chi.URLParam(r, "appName")
	if err := a.Manager.TeardownApp(r.Context(), appName); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(ErrResponse{HTTPStatusCode: 404, Message: "app not found"})
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(ErrResponse{HTTPStatusCode: 500, Message: err.Error()})
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// ---------------------------------------------------------------------------
// Deploy / teardown logic
// ---------------------------------------------------------------------------

// DeployResult is the response returned after a successful app deployment.
type DeployResult struct {
	App      string                       `json:"app"`
	Status   string                       `json:"status"`
	Services map[string]DeployServiceInfo `json:"services"`
}

// DeployServiceInfo describes a deployed service's address.
type DeployServiceInfo struct {
	TaskID   string `json:"task_id"`
	WorkerID string `json:"worker_id"`
	Address  string `json:"address"` // host:port reachable from LAN
}

// DeployApp deploys a manifest's services in topological order, injecting
// service discovery env vars for each dependency that is already running.
func (m *Manager) DeployApp(ctx context.Context, mf *manifest.Manifest) (*DeployResult, error) {
	if m.Store == nil {
		return nil, errors.New("store not configured")
	}

	// Build AppGroup from manifest for topo ordering + store.
	ag, err := manifest.ToAppGroup(mf)
	if err != nil {
		return nil, fmt.Errorf("building dependency graph: %w", err)
	}

	appGroupCtx, agCancel := context.WithTimeout(ctx, 5*time.Second)
	_ = m.Store.CreateAppGroup(appGroupCtx, ag) // best effort, may already exist
	agCancel()

	order, err := ag.TopologicalOrder()
	if err != nil {
		return nil, fmt.Errorf("topological sort: %w", err)
	}

	tasks := manifest.ToTasks(mf, mf.Name)

	// Create App record.
	app := &store.App{
		Name:         mf.Name,
		ServiceTasks: make(map[string]string),
		Status:       "deploying",
	}
	appCtx, appCancel := context.WithTimeout(ctx, 5*time.Second)
	if err := m.Store.CreateApp(appCtx, app); err != nil {
		appCancel()
		return nil, fmt.Errorf("creating app record: %w", err)
	}
	appCancel()

	// Track discovered service addresses: serviceName → {host, port}
	type svcAddr struct {
		Host string
		Port string
	}
	discovery := make(map[string]svcAddr)
	result := &DeployResult{
		App:      mf.Name,
		Status:   "running",
		Services: make(map[string]DeployServiceInfo),
	}

	for _, svcName := range order {
		t, ok := tasks[svcName]
		if !ok {
			continue
		}

		// Inject discovery env vars from already-deployed dependencies.
		svc := mf.Services[svcName]
		for _, dep := range svc.DependsOn {
			if addr, found := discovery[dep]; found {
				prefix := manifest.ServiceEnvKey(dep)
				t.Env = append(t.Env, fmt.Sprintf("%s_HOST=%s", prefix, addr.Host))
				t.Env = append(t.Env, fmt.Sprintf("%s_PORT=%s", prefix, addr.Port))
			}
		}

		// Persist the task in Pending state.
		createCtx, createCancel := context.WithTimeout(ctx, 5*time.Second)
		if err := m.Store.CreateTask(createCtx, t, ""); err != nil {
			createCancel()
			result.Status = "partial"
			log.Printf("Manager %s: failed to create task for service %s: %v", m.ID, svcName, err)
			break
		}
		createCancel()

		// Record in app.
		app.ServiceTasks[svcName] = t.ID.String()
		updateCtx, updateCancel := context.WithTimeout(ctx, 5*time.Second)
		_ = m.Store.UpdateApp(updateCtx, app)
		updateCancel()

		// Wait for the task to reach Running state.
		if err := m.waitForTaskRunning(ctx, t.ID, 120*time.Second); err != nil {
			log.Printf("Manager %s: service %s did not reach Running: %v", m.ID, svcName, err)
			result.Status = "partial"
			result.Services[svcName] = DeployServiceInfo{TaskID: t.ID.String(), WorkerID: "", Address: "pending"}
			continue
		}

		// Resolve the worker address and mapped port.
		host, port, workerID := m.resolveServiceAddress(ctx, t.ID, svc.Ports)
		discovery[svcName] = svcAddr{Host: host, Port: port}
		result.Services[svcName] = DeployServiceInfo{
			TaskID:   t.ID.String(),
			WorkerID: workerID,
			Address:  fmt.Sprintf("%s:%s", host, port),
		}

		log.Printf("Manager %s: service %s deployed at %s:%s on worker %s", m.ID, svcName, host, port, workerID)
	}

	if result.Status == "running" {
		app.Status = "running"
	} else {
		app.Status = result.Status
	}
	finalCtx, finalCancel := context.WithTimeout(ctx, 5*time.Second)
	_ = m.Store.UpdateApp(finalCtx, app)
	finalCancel()

	return result, nil
}

// waitForTaskRunning polls the store until the task reaches Running state or
// the timeout elapses.
func (m *Manager) waitForTaskRunning(ctx context.Context, taskID uuid.UUID, timeout time.Duration) error {
	deadline := time.After(timeout)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline:
			return fmt.Errorf("timeout waiting for task %s to become Running", taskID)
		case <-ticker.C:
			checkCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			t, _, err := m.Store.GetTask(checkCtx, taskID)
			cancel()
			if err != nil {
				continue
			}
			if t.State == task.Running {
				return nil
			}
			if t.State == task.Failed {
				return fmt.Errorf("task %s failed", taskID)
			}
		}
	}
}

// resolveServiceAddress returns the worker's IP and the first mapped host port
// for the given task.
func (m *Manager) resolveServiceAddress(ctx context.Context, taskID uuid.UUID, declaredPorts map[string]string) (host string, port string, workerID string) {
	checkCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	t, wID, err := m.Store.GetTask(checkCtx, taskID)
	cancel()
	if err != nil {
		return "unknown", "0", ""
	}
	workerID = wID

	// Extract host IP from worker address (format: "ip:port").
	if workerID != "" {
		parts := strings.SplitN(workerID, ":", 2)
		host = parts[0]
	}
	if host == "" {
		host = "unknown"
	}

	// Try to get the actual mapped host port from HostPorts (set by worker
	// after container inspection). Fall back to declared port binding.
	if t != nil && t.HostPorts != nil {
		for _, bindings := range t.HostPorts {
			if len(bindings) > 0 && bindings[0].HostPort != "" {
				port = bindings[0].HostPort
				return
			}
		}
	}

	// Fallback: use the first declared port binding value.
	for _, hp := range declaredPorts {
		port = hp
		return
	}

	port = "0"
	return
}

// TeardownApp stops all services in an app in reverse topological order and
// removes the app record.
func (m *Manager) TeardownApp(ctx context.Context, appName string) error {
	if m.Store == nil {
		return errors.New("store not configured")
	}

	getCtx, getCancel := context.WithTimeout(ctx, 5*time.Second)
	app, err := m.Store.GetApp(getCtx, appName)
	getCancel()
	if err != nil {
		return err
	}

	app.Status = "stopping"
	updateCtx, updateCancel := context.WithTimeout(ctx, 5*time.Second)
	_ = m.Store.UpdateApp(updateCtx, app)
	updateCancel()

	// Try to get topological order for reverse teardown.
	agCtx, agCancel := context.WithTimeout(ctx, 5*time.Second)
	ag, agErr := m.Store.GetAppGroup(agCtx, appName)
	agCancel()

	var stopOrder []string
	if agErr == nil {
		order, err := ag.TopologicalOrder()
		if err == nil {
			// Reverse the order: stop dependents first.
			for i := len(order) - 1; i >= 0; i-- {
				stopOrder = append(stopOrder, order[i])
			}
		}
	}

	// If we couldn't get topo order, just iterate the map.
	if len(stopOrder) == 0 {
		for svcName := range app.ServiceTasks {
			stopOrder = append(stopOrder, svcName)
		}
	}

	for _, svcName := range stopOrder {
		taskIDStr, ok := app.ServiceTasks[svcName]
		if !ok {
			continue
		}

		tID, err := uuid.Parse(taskIDStr)
		if err != nil {
			continue
		}

		stopCtx, stopCancel := context.WithTimeout(ctx, 5*time.Second)
		_, workerID, err := m.Store.GetTask(stopCtx, tID)
		stopCancel()
		if err != nil {
			log.Printf("Manager %s: teardown: could not find task %s for service %s: %v", m.ID, taskIDStr, svcName, err)
			continue
		}

		if workerID != "" {
			m.stopTask(workerID, taskIDStr)
		}
		log.Printf("Manager %s: teardown: stopped service %s (task %s)", m.ID, svcName, taskIDStr)
	}

	app.Status = "stopped"
	delCtx, delCancel := context.WithTimeout(ctx, 5*time.Second)
	_ = m.Store.DeleteApp(delCtx, appName)
	delCancel()

	return nil
}

func (a *Api) initRouter() {
	a.Router = chi.NewRouter()
	a.Router.Get("/status", a.StatusHandler)
	a.Router.Route("/tasks", func(r chi.Router) {
		r.Post("/", a.StartTaskHandler)
		r.Get("/", a.GetTasksHandler)
		r.Route("/{taskID}", func(r chi.Router) {
			r.Delete("/", a.StopTaskHandler)
		})
	})
	a.Router.Route("/workers", func(r chi.Router) {
		r.Post("/", a.RegisterWorkerHandler)
		r.Route("/{workerID}", func(r chi.Router) {
			r.Put("/heartbeat", a.HeartbeatHandler)
		})
	})
	a.Router.Get("/nodes", a.GetNodesHandler)
	a.Router.Route("/apps", func(r chi.Router) {
		r.Post("/", a.DeployAppHandler)
		r.Get("/", a.ListAppsHandler)
		r.Route("/{appName}", func(r chi.Router) {
			r.Get("/", a.GetAppHandler)
			r.Delete("/", a.DeleteAppHandler)
		})
	})
}

func (a *Api) Start() {
	a.initRouter()
	http.ListenAndServe(fmt.Sprintf("%s:%d", a.Address, a.Port), a.Router)
}

func (m *Manager) UpdateTasks() {
	for {
		if !m.IsLeader() {
			log.Printf("Manager %s is in follower role; skipping worker task sync", m.ID)
			time.Sleep(15 * time.Second)
			continue
		}
		log.Println("Checking for task updates from workers")
		m.updateTasks()
		log.Println("Tasks update completed")
		log.Println("Sleeping for 15 seconds")
		time.Sleep(15 * time.Second)
	}
}

func (m *Manager) ProcessTasks() {
	for {
		if !m.IsLeader() {
			log.Printf("Manager %s is in follower role; skipping task processing", m.ID)
			time.Sleep(10 * time.Second)
			continue
		}
		log.Println("Processing any tasks in the queue")
		m.SendWork()
		log.Printf("Sleeping for 10 seconds")
		time.Sleep(10 * time.Second)
	}
}

func (m *Manager) getHostPort(ports nat.PortMap) *string {
	for k, _ := range ports {
		return &ports[k][0].HostPort
	}

	return nil
}

func (m *Manager) checkTaskHealth(t task.Task) error {
	log.Printf("Calling health check for task %s: %s\n", t.ID, t.HealthCheck)
	if m.Store == nil {
		return errors.New("store not configured")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, workerID, err := m.Store.GetTask(ctx, t.ID)
	cancel()
	if err != nil {
		msg := fmt.Sprintf("Error retrieving worker for task %s: %v", t.ID, err)
		log.Println(msg)
		return errors.New(msg)
	}

	if workerID == "" {
		msg := fmt.Sprintf("No worker assigned for task %s", t.ID)
		log.Println(msg)
		return errors.New(msg)
	}

	hostPort := m.getHostPort(t.HostPorts)
	if hostPort == nil {
		msg := fmt.Sprintf("No host port found for task %s", t.ID)
		log.Println(msg)
		return errors.New(msg)
	}

	worker := strings.Split(workerID, ":")
	url := fmt.Sprintf("http://%s:%s%s", worker[0], *hostPort, t.HealthCheck)

	log.Printf("Calling health check for task %s: %s\n", t.ID, url)

	resp, err := http.Get(url)

	if err != nil {
		msg := fmt.Sprintf("Error connecting to health check %s", url)
		log.Println(msg)
		return errors.New(msg)
	}

	if resp.StatusCode != http.StatusOK {
		msg := fmt.Sprintf("Error health check for task %s did not return 200\n", t.ID)
		log.Println(msg)
		return errors.New(msg)
	}

	log.Printf("Task %s health check response: %v\n", t.ID, resp.StatusCode)

	return nil

}

func (m *Manager) doHealthChecks() {
	for _, t := range m.GetTasks() {
		if t.State == task.Running && t.RestartCount < 3 {
			// Only check health if a health check URL is defined
			if t.HealthCheck != "" {
				err := m.checkTaskHealth(*t)
				if err != nil {
					if t.RestartCount < 3 {
						m.restartTask(t)
					}
				}
			}
		} else if t.State == task.Failed && t.RestartCount < 3 {
			m.restartTask(t)
		}
	}
}

func (m *Manager) restartTask(t *task.Task) {
	if !m.IsLeader() {
		log.Printf("Manager %s is in follower role; skipping restart for task %s", m.ID, t.ID)
		return
	}
	if m.Store == nil {
		log.Println("Store not configured; cannot restart task")
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, workerID, err := m.Store.GetTask(ctx, t.ID)
	cancel()
	if err != nil {
		log.Printf("Error fetching task %s for restart: %v", t.ID, err)
		return
	}

	if workerID == "" {
		log.Printf("No worker assignment found for task %s; cannot restart", t.ID)
		return
	}

	workerCtx, workerCancel := context.WithTimeout(context.Background(), 5*time.Second)
	workers, err := m.activeWorkers(workerCtx)
	workerCancel()
	if err != nil {
		log.Printf("Error listing workers for restart of task %s: %v", t.ID, err)
		return
	}

	var assignedWorker *store.Worker
	for _, w := range workers {
		if w.ID == workerID {
			copy := w
			assignedWorker = &copy
			break
		}
	}

	if assignedWorker == nil {
		log.Printf("Assigned worker %s for task %s not found among active workers", workerID, t.ID)
		m.resetTaskToPending(*t)
		return
	}

	t.State = task.Scheduled
	t.RestartCount++

	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	if err := m.Store.UpdateTaskState(ctx, t, workerID); err != nil {
		log.Printf("Error persisting restart state for task %s: %v", t.ID, err)
	}
	cancel()

	m.dispatchTaskToWorker(*t, assignedWorker)

}

func (m *Manager) DoHealthChecks() {
	for {
		if !m.IsLeader() {
			log.Printf("Manager %s is in follower role; skipping health checks", m.ID)
			time.Sleep(60 * time.Second)
			continue
		}
		log.Println("Performing task health check")
		m.doHealthChecks()
		log.Println("Task health checks completed")
		log.Println("Sleeping for 60 seconds")
		time.Sleep(60 * time.Second)
	}
}

func (m *Manager) stopTask(worker string, taskID string) {
	if !m.IsLeader() {
		log.Printf("Manager %s is in follower role; skipping stop for task %s", m.ID, taskID)
		return
	}
	err := m.WorkerClient.StopTask(worker, taskID)
	if err != nil {
		log.Printf("Error sending request: %v\n", err)
		return
	}
	if id, err := uuid.Parse(taskID); err == nil {
		if m.Store == nil {
			log.Println("Store not configured; cannot persist stop")
			return
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		t, _, err := m.Store.GetTask(ctx, id)
		cancel()
		if err != nil {
			log.Printf("Error fetching task %s to persist stop: %v", taskID, err)
			return
		}

		t.State = task.Completed
		t.EndTime = time.Now().UTC()

		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
		if err := m.Store.UpdateTaskState(ctx, t, worker); err != nil {
			log.Printf("Error persisting stop for task %s: %v", taskID, err)
		}
		cancel()
	}

	log.Printf("Task %s has been scheduled to be stopped", taskID)
}
