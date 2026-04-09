package worker

import (
    "bytes"
    "context"
    "encoding/json"
    "fmt"
    "log"
    "net"
    "net/http"
    "time"

    "github.com/aditip149209/okube/pkg/store"
)

// heartbeatClient is a shared HTTP client for heartbeat requests. Reusing a
// single client allows connection pooling and avoids exhausting ephemeral
// ports under high heartbeat frequency.
var heartbeatClient = &http.Client{
    Timeout: 10 * time.Second,
    Transport: &http.Transport{
        // Prevent the client from following redirects to unreachable
        // addresses (e.g. 0.0.0.0) that the manager may emit during
        // brief leader-election transitions.
        DialContext: (&net.Dialer{
            Timeout:   5 * time.Second,
            KeepAlive: 30 * time.Second,
        }).DialContext,
        MaxIdleConns:        5,
        IdleConnTimeout:     60 * time.Second,
    },
    // Do not follow redirects automatically. If the manager responds with
    // a redirect (e.g. HTTP 307 to the leader), the Location may contain
    // a non-routable address like 0.0.0.0. Instead, return the redirect
    // response so the caller can retry on the next heartbeat tick.
    CheckRedirect: func(req *http.Request, via []*http.Request) error {
        return http.ErrUseLastResponse
    },
}

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

    client := &http.Client{Timeout: 10 * time.Second}
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

    resp, err := heartbeatClient.Do(req)
    if err != nil {
        return fmt.Errorf("heartbeat to %s failed: %w", managerAddress, err)
    }
    defer resp.Body.Close()

    if resp.StatusCode == http.StatusTemporaryRedirect || resp.StatusCode == http.StatusServiceUnavailable {
        return fmt.Errorf("manager at %s not ready (status %d), will retry", managerAddress, resp.StatusCode)
    }

    if resp.StatusCode != http.StatusNoContent {
        return fmt.Errorf("unexpected status code %d during heartbeat to %s", resp.StatusCode, managerAddress)
    }

    return nil
}

func StartHeartbeat(ctx context.Context, managerAddress, workerID string, interval time.Duration) {
    if interval <= 0 {
        interval = 10 * time.Second
    }

    log.Printf("Starting heartbeat for worker %s to manager at %s (interval %s)", workerID, managerAddress, interval)

    ticker := time.NewTicker(interval)
    defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            heartbeatCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
            if err := sendHeartbeat(heartbeatCtx, managerAddress, workerID); err != nil {
                log.Printf("Error sending heartbeat for worker %s to %s: %v", workerID, managerAddress, err)
            }
            cancel()
        }
    }
}
