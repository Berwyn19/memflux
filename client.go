package memflux

import (
	"context"
	"log"
	pb "memflux/proto"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Follower struct {
	address string
	client  pb.ReplicationClient
	healthy bool
}

type LeaderClient struct {
	followers []*Follower
	sequence  int64
	mu        sync.Mutex
}

func NewLeaderClient(addresses []string) *LeaderClient {
	lc := &LeaderClient{}
	for _, address := range addresses {
		conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("failed to connect to follower %s: %v", address, err)
			continue
		}
		lc.followers = append(lc.followers, &Follower{
			address: address,
			client:  pb.NewReplicationClient(conn),
			healthy: true,
		})
	}
	go lc.recoverFollowers()
	return lc
}

func (lc *LeaderClient) Replicate(operation, key, value string, ttlSeconds int64) {
	req := &pb.ReplicateRequest{
		Key:        key,
		Value:      value,
		TtlSeconds: ttlSeconds,
		Sequence:   atomic.AddInt64(&lc.sequence, 1),
		Operation:  operation,
	}

	for _, follower := range lc.followers {
		lc.mu.Lock()
		healthy := follower.healthy
		lc.mu.Unlock()
		if !healthy { // BUG-2 fix: was `if healthy`
			continue
		}

		success := false
		for i := 0; i < 3; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			resp, err := follower.client.Replicate(ctx, req)
			if err == nil && resp.Success {
				success = true
				cancel()
				break
			}
			log.Printf("failed to replicate to %s attempt %d: %v", follower.address, i+1, err)
			cancel()
		}

		if !success {
			lc.mu.Lock()
			follower.healthy = false
			lc.mu.Unlock()
			log.Printf("marking follower %s as unhealthy", follower.address)
		}
	}
}

// recoverFollowers probes unhealthy followers every 10 seconds and restores
// them when they respond to a heartbeat. (BUG-4 fix)
func (lc *LeaderClient) recoverFollowers() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		for _, follower := range lc.followers {
			lc.mu.Lock()
			healthy := follower.healthy
			lc.mu.Unlock()
			if healthy {
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			_, err := follower.client.HeartBeat(ctx, &pb.HeartBeatRequest{})
			cancel()
			if err == nil {
				lc.mu.Lock()
				follower.healthy = true
				lc.mu.Unlock()
				log.Printf("follower %s recovered, marking healthy", follower.address)
			}
		}
	}
}
