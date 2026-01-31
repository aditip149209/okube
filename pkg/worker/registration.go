package worker

import (
    "bytes"
    "context"
    "encoding/json"
    "fmt"
    "log"
    "net/http"
    "time"

    "github.com/aditip149209/okube/pkg/store"
)

func RegisterWithManager(ctx context.Context, managerAddress string, meta store.Worker) error {
    meta.Heartbeat = time.Now().UTC()

    payload, err := json.Marshal(meta)
    if err != nil {
        return err
    }

    req, err := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("http://%s/workers", managerAddress), bytes.NewBuffer(payload))
    if err != nil {
        return err
    }
    req.Header.Set("Content-Type", "application/json")

    client := &http.Client{Timeout: 5 * time.Second}
    resp, err := client.Do(req)
    if err != nil {
        return err
    }
    defer resp.Body.Close()

    if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
        return fmt.Errorf("unexpected status code %d during registration", resp.StatusCode)
    }

    return nil
}

func sendHeartbeat(ctx context.Context, managerAddress, workerID string) error {
    req, err := http.NewRequestWithContext(ctx, http.MethodPut, fmt.Sprintf("http://%s/workers/%s/heartbeat", managerAddress, workerID), nil)
    if err != nil {
        return err
    }

    client := &http.Client{Timeout: 5 * time.Second}
    resp, err := client.Do(req)
    if err != nil {
        return err
    }
    defer resp.Body.Close()

    if resp.StatusCode != http.StatusNoContent {
        return fmt.Errorf("unexpected status code %d during heartbeat", resp.StatusCode)
    }

    return nil
}

func StartHeartbeat(ctx context.Context, managerAddress, workerID string, interval time.Duration) {
    if interval <= 0 {
        interval = 10 * time.Second
    }

    ticker := time.NewTicker(interval)
    defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            heartbeatCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
            if err := sendHeartbeat(heartbeatCtx, managerAddress, workerID); err != nil {
                log.Printf("Error sending heartbeat for worker %s: %v", workerID, err)
            }
            cancel()
        }
    }
}
