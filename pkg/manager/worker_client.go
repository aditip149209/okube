package manager

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/aditip149209/okube/pkg/task"
	workerpkg "github.com/aditip149209/okube/pkg/worker"
)

type WorkerCommunicator interface {
	FetchTasks(worker string) ([]*task.Task, error)
	StartTask(worker string, event task.TaskEvent) (*task.Task, *workerpkg.ErrResponse, error)
	StopTask(worker string, taskID string) error
}

type HTTPWorkerClient struct {
	HTTPClient *http.Client
}

func NewHTTPWorkerClient(client *http.Client) *HTTPWorkerClient {
	if client == nil {
		client = &http.Client{}
	}
	return &HTTPWorkerClient{HTTPClient: client}
}

func (h *HTTPWorkerClient) FetchTasks(worker string) ([]*task.Task, error) {
	url := fmt.Sprintf("http://%s/tasks", worker)
	resp, err := h.HTTPClient.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code %d from %s", resp.StatusCode, url)
	}

	decoder := json.NewDecoder(resp.Body)
	var tasks []*task.Task
	if err := decoder.Decode(&tasks); err != nil {
		return nil, err
	}

	return tasks, nil
}

func (h *HTTPWorkerClient) StartTask(worker string, event task.TaskEvent) (*task.Task, *workerpkg.ErrResponse, error) {
	url := fmt.Sprintf("http://%s/tasks", worker)
	payload, err := json.Marshal(event)
	if err != nil {
		return nil, nil, err
	}

	resp, err := h.HTTPClient.Post(url, "application/json", bytes.NewBuffer(payload))
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()

	decoder := json.NewDecoder(resp.Body)
	if resp.StatusCode != http.StatusCreated {
		respErr := workerpkg.ErrResponse{}
		if err := decoder.Decode(&respErr); err != nil {
			return nil, nil, err
		}
		return nil, &respErr, nil
	}

	t := task.Task{}
	if err := decoder.Decode(&t); err != nil {
		return nil, nil, err
	}

	return &t, nil, nil
}

func (h *HTTPWorkerClient) StopTask(worker string, taskID string) error {
	url := fmt.Sprintf("http://%s/tasks/%s", worker, taskID)
	request, err := http.NewRequest(http.MethodDelete, url, nil)
	if err != nil {
		return err
	}

	resp, err := h.HTTPClient.Do(request)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("unexpected status code %d from %s", resp.StatusCode, url)
	}

	return nil
}
