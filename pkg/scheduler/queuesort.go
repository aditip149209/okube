package scheduler

import (
	"sort"
	"strings"

	"github.com/aditip149209/okube/pkg/appgroup"
	"github.com/aditip149209/okube/pkg/task"
)

// QueueSortStrategy controls ordering inside the queue sort stage.
type QueueSortStrategy string

const (
	QueueSortKahn          QueueSortStrategy = "kahn"
	QueueSortReverseKahn   QueueSortStrategy = "reversekahn"
	QueueSortAlternateKahn QueueSortStrategy = "alternatekahn"
)

// ParseQueueSortStrategy normalizes and validates queue sort strategy values.
func ParseQueueSortStrategy(raw string) QueueSortStrategy {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case string(QueueSortReverseKahn):
		return QueueSortReverseKahn
	case string(QueueSortAlternateKahn):
		return QueueSortAlternateKahn
	default:
		return QueueSortKahn
	}
}

// QueueSorter reorders pending tasks using AppGroup dependency topology.
type QueueSorter struct {
	Strategy QueueSortStrategy
}

func NewQueueSorter(strategy string) *QueueSorter {
	return &QueueSorter{Strategy: ParseQueueSortStrategy(strategy)}
}

// SortPendingTasks applies dependency-aware ordering. Tasks not mapped to a
// known AppGroup stay in their original relative order and are appended after
// sorted AppGroup tasks.
func (qs *QueueSorter) SortPendingTasks(tasks []*task.Task, groups []*appgroup.AppGroup) []*task.Task {
	if len(tasks) < 2 {
		return tasks
	}
	if qs == nil {
		qs = NewQueueSorter("")
	}

	groupByID := make(map[string]*appgroup.AppGroup, len(groups))
	for _, g := range groups {
		if g != nil && g.AppID != "" {
			groupByID[g.AppID] = g
		}
	}

	tasksByGroup := make(map[string][]*task.Task)
	groupOrder := make([]string, 0)
	leftovers := make([]*task.Task, 0)

	for _, t := range tasks {
		if t == nil || t.AppID == "" {
			leftovers = append(leftovers, t)
			continue
		}
		g, ok := groupByID[t.AppID]
		if !ok || g == nil {
			leftovers = append(leftovers, t)
			continue
		}

		serviceID := serviceIDForTask(t)
		if serviceID == "" {
			leftovers = append(leftovers, t)
			continue
		}

		if _, seen := tasksByGroup[t.AppID]; !seen {
			groupOrder = append(groupOrder, t.AppID)
		}
		tasksByGroup[t.AppID] = append(tasksByGroup[t.AppID], t)
	}

	ordered := make([]*task.Task, 0, len(tasks))
	for _, appID := range groupOrder {
		g := groupByID[appID]
		ordered = append(ordered, qs.sortGroupTasks(tasksByGroup[appID], g)...)
	}
	ordered = append(ordered, leftovers...)
	return ordered
}

func (qs *QueueSorter) sortGroupTasks(groupTasks []*task.Task, g *appgroup.AppGroup) []*task.Task {
	if len(groupTasks) < 2 || g == nil {
		return groupTasks
	}

	serviceToTasks := make(map[string][]*task.Task)
	servicePresent := make(map[string]struct{})
	for _, t := range groupTasks {
		sid := serviceIDForTask(t)
		if sid == "" {
			continue
		}
		serviceToTasks[sid] = append(serviceToTasks[sid], t)
		servicePresent[sid] = struct{}{}
	}

	serviceRank := make(map[string]int)
	services := qs.buildServiceOrder(g, servicePresent)
	for i, sid := range services {
		serviceRank[sid] = i
	}

	type rankedTask struct {
		task          *task.Task
		rank          int
		satisfiedDeps int
		originalIndex int
	}

	ranked := make([]rankedTask, 0, len(groupTasks))
	for idx, t := range groupTasks {
		sid := serviceIDForTask(t)
		rank, ok := serviceRank[sid]
		if !ok {
			rank = len(serviceRank) + idx
		}
		ranked = append(ranked, rankedTask{
			task:          t,
			rank:          rank,
			satisfiedDeps: countSatisfiedDependencies(g, sid, servicePresent),
			originalIndex: idx,
		})
	}

	sort.SliceStable(ranked, func(i, j int) bool {
		if ranked[i].satisfiedDeps != ranked[j].satisfiedDeps {
			return ranked[i].satisfiedDeps < ranked[j].satisfiedDeps
		}
		if ranked[i].rank != ranked[j].rank {
			return ranked[i].rank < ranked[j].rank
		}
		return ranked[i].originalIndex < ranked[j].originalIndex
	})

	result := make([]*task.Task, 0, len(groupTasks))
	for _, r := range ranked {
		result = append(result, r.task)
	}
	return result
}

// buildServiceOrder returns service ordering according to selected strategy.
func (qs *QueueSorter) buildServiceOrder(g *appgroup.AppGroup, servicePresent map[string]struct{}) []string {
	// Build dependency->dependent graph for only services present in pending queue.
	adj := make(map[string][]string)
	inDegree := make(map[string]int)

	for sid := range servicePresent {
		adj[sid] = nil
		inDegree[sid] = 0
	}

	for sid := range servicePresent {
		deps := g.GetDependencies(sid)
		for _, e := range deps {
			dep := e.To
			if _, ok := servicePresent[dep]; !ok {
				continue
			}
			adj[dep] = append(adj[dep], sid)
			inDegree[sid]++
		}
	}

	queue := make([]string, 0)
	for sid, deg := range inDegree {
		if deg == 0 {
			queue = append(queue, sid)
		}
	}
	sort.Strings(queue)

	order := make([]string, 0, len(servicePresent))
	for len(queue) > 0 {
		curr := queue[0]
		queue = queue[1:]
		order = append(order, curr)

		next := adj[curr]
		sort.Strings(next)
		for _, depd := range next {
			inDegree[depd]--
			if inDegree[depd] == 0 {
				queue = append(queue, depd)
			}
		}
	}

	// If cycles or disconnected bookkeeping gaps occurred, append missing nodes.
	if len(order) < len(servicePresent) {
		missing := make([]string, 0)
		for sid := range servicePresent {
			if !contains(order, sid) {
				missing = append(missing, sid)
			}
		}
		sort.Strings(missing)
		order = append(order, missing...)
	}

	switch qs.Strategy {
	case QueueSortReverseKahn:
		reverseStrings(order)
	case QueueSortAlternateKahn:
		order = alternateStrings(order)
	}

	return order
}

func countSatisfiedDependencies(g *appgroup.AppGroup, serviceID string, pending map[string]struct{}) int {
	if g == nil || serviceID == "" {
		return 0
	}
	deps := g.GetDependencies(serviceID)
	satisfied := 0
	for _, e := range deps {
		if _, stillPending := pending[e.To]; !stillPending {
			satisfied++
		}
	}
	return satisfied
}

func serviceIDForTask(t *task.Task) string {
	if t == nil {
		return ""
	}
	if t.ServiceID != "" {
		return t.ServiceID
	}
	return t.Name
}

func reverseStrings(items []string) {
	for l, r := 0, len(items)-1; l < r; l, r = l+1, r-1 {
		items[l], items[r] = items[r], items[l]
	}
}

func alternateStrings(items []string) []string {
	out := make([]string, 0, len(items))
	l, r := 0, len(items)-1
	for l <= r {
		out = append(out, items[l])
		if l != r {
			out = append(out, items[r])
		}
		l++
		r--
	}
	return out
}

func contains(items []string, target string) bool {
	for _, item := range items {
		if item == target {
			return true
		}
	}
	return false
}
