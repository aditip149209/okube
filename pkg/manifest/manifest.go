package manifest

import (
	"fmt"
	"os"
	"strings"

	"github.com/aditip149209/okube/pkg/appgroup"
	"github.com/aditip149209/okube/pkg/task"
	"github.com/docker/go-connections/nat"
	"github.com/google/uuid"
	"gopkg.in/yaml.v3"
)

// ServiceResources defines optional resource requests for a service.
type ServiceResources struct {
	Memory int64   `yaml:"memory" json:"memory"`
	Disk   int64   `yaml:"disk" json:"disk"`
	CPU    float64 `yaml:"cpu" json:"cpu"`
}

// ServiceSpec describes a single service within a manifest.
type ServiceSpec struct {
	Image       string            `yaml:"image" json:"image"`
	Ports       map[string]string `yaml:"ports" json:"ports"`
	Env         map[string]string `yaml:"env,omitempty" json:"env,omitempty"`
	Volumes     []string          `yaml:"volumes,omitempty" json:"volumes,omitempty"`
	DependsOn   []string          `yaml:"dependsOn,omitempty" json:"dependsOn,omitempty"`
	HealthCheck string            `yaml:"healthCheck,omitempty" json:"healthCheck,omitempty"`
	Command     []string          `yaml:"command,omitempty" json:"command,omitempty"`
	Resources   ServiceResources  `yaml:"resources,omitempty" json:"resources,omitempty"`
}

// Manifest is a declarative multi-service application definition.
type Manifest struct {
	Name     string                 `yaml:"name" json:"name"`
	Services map[string]ServiceSpec `yaml:"services" json:"services"`
}

// ParseManifestFile reads and parses a YAML manifest from the given file path.
func ParseManifestFile(filePath string) (*Manifest, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("reading manifest: %w", err)
	}
	return ParseManifest(data)
}

// ParseManifest parses a YAML manifest from raw bytes.
func ParseManifest(data []byte) (*Manifest, error) {
	var m Manifest
	if err := yaml.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("parsing manifest YAML: %w", err)
	}

	if m.Name == "" {
		return nil, fmt.Errorf("manifest name is required")
	}
	if len(m.Services) == 0 {
		return nil, fmt.Errorf("manifest must define at least one service")
	}

	for name, svc := range m.Services {
		if svc.Image == "" {
			return nil, fmt.Errorf("service %q: image is required", name)
		}
		for _, dep := range svc.DependsOn {
			if _, ok := m.Services[dep]; !ok {
				return nil, fmt.Errorf("service %q depends on unknown service %q", name, dep)
			}
		}
	}

	return &m, nil
}

// ToAppGroup converts a manifest's dependency graph into an AppGroup.
func ToAppGroup(m *Manifest) (*appgroup.AppGroup, error) {
	services := make([]string, 0, len(m.Services))
	for name := range m.Services {
		services = append(services, name)
	}

	var edges []appgroup.DependencyEdge
	for name, svc := range m.Services {
		for _, dep := range svc.DependsOn {
			edges = append(edges, appgroup.DependencyEdge{
				From: name,
				To:   dep,
			})
		}
	}

	return appgroup.NewAppGroup(m.Name, services, edges)
}

// ToTasks converts each service in the manifest to a Task. The returned map
// is keyed by service name.
func ToTasks(m *Manifest, appID string) map[string]*task.Task {
	tasks := make(map[string]*task.Task, len(m.Services))

	for name, svc := range m.Services {
		exposedPorts := make(nat.PortSet)
		portBindings := make(map[string]string)
		for containerPort, hostPort := range svc.Ports {
			p := nat.Port(containerPort)
			exposedPorts[p] = struct{}{}
			portBindings[containerPort] = hostPort
		}

		var envSlice []string
		for k, v := range svc.Env {
			envSlice = append(envSlice, fmt.Sprintf("%s=%s", k, v))
		}

		t := &task.Task{
			ID:           uuid.New(),
			Name:         fmt.Sprintf("%s-%s", m.Name, name),
			AppID:        appID,
			ServiceID:    name,
			State:        task.Pending,
			Image:        svc.Image,
			Memory:       int(svc.Resources.Memory),
			Disk:         int(svc.Resources.Disk),
			ExposedPorts: exposedPorts,
			PortBindings: portBindings,
			HealthCheck:  svc.HealthCheck,
			Env:          envSlice,
			Volumes:      svc.Volumes,
			Command:      svc.Command,
		}

		tasks[name] = t
	}

	return tasks
}

// ServiceEnvKey returns the environment variable prefix for a service name.
// e.g. "db" -> "DB", "my-backend" -> "MY_BACKEND"
func ServiceEnvKey(serviceName string) string {
	s := strings.ToUpper(serviceName)
	s = strings.ReplaceAll(s, "-", "_")
	s = strings.ReplaceAll(s, ".", "_")
	return s
}
