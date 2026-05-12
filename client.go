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
	leaderClient := LeaderClient{}
	for _, address := range addresses {
		conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("failed to connect to follower %s: %v", address, err)
			continue
		}
		client := pb.NewReplicationClient(conn)
		follower := &Follower{
			address: address,
			client:  client,
			healthy: true,
		}
		leaderClient.followers = append(leaderClient.followers, follower)
	}
	return &leaderClient
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
		if healthy {
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
