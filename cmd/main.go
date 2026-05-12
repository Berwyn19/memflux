package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"memflux"
	pb "memflux/proto"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// 1. parse flags
	port := flag.String("port", "8001", "gRPC port to listen on")
	httpPort := flag.String("http-port", "", "HTTP API port (defaults to gRPC port + 1000)")
	role := flag.String("role", "leader", "leader or follower")
	leader := flag.String("leader", "", "leader address for followers")
	followers := flag.String("followers", "", "comma-separated follower addresses for leader")
	flag.Parse()

	if *httpPort == "" {
		p, _ := strconv.Atoi(*port)
		*httpPort = strconv.Itoa(p + 1000)
	}

	// 2. create store
	store := memflux.NewStore()

	// 3. create replication server
	nodeID := "node-" + *port
	server := memflux.NewReplicationServer(store, nodeID)

	// 4. start gRPC server
	lis, err := net.Listen("tcp", ":"+*port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterReplicationServer(grpcServer, server)
	go func() {
		log.Printf("[%s] gRPC server listening on :%s as %s", nodeID, *port, *role)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("gRPC server failed: %v", err)
		}
	}()

	// 5. if leader → create leader client
	var lc *memflux.LeaderClient // BUG-3 fix: declared at function scope so HTTP handlers can use it
	if *role == "leader" && *followers != "" {
		addresses := strings.Split(*followers, ",")
		lc = memflux.NewLeaderClient(addresses)
		log.Printf("[%s] connected to followers: %v", nodeID, addresses)
	}

	// 6. if follower → start heartbeat goroutine
	if *role == "follower" && *leader != "" {
		go func() {
			conn, err := grpc.NewClient(*leader, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Printf("[%s] failed to connect to leader: %v", nodeID, err)
				return
			}
			defer conn.Close()
			client := pb.NewReplicationClient(conn)
			ticker := time.NewTicker(5 * time.Second)
			defer ticker.Stop()
			for range ticker.C {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				_, err := client.HeartBeat(ctx, &pb.HeartBeatRequest{LeaderId: nodeID})
				cancel()
				if err != nil {
					log.Printf("[%s] leader unreachable: %v", nodeID, err)
				}
			}
		}()
	}

	// HTTP API — all nodes serve reads; only the leader propagates writes via lc
	go startHTTP(nodeID, *httpPort, store, lc)

	select {}
}

func startHTTP(nodeID, port string, store *memflux.Store, lc *memflux.LeaderClient) {
	mux := http.NewServeMux()

	mux.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		val, ok := store.Get(key)
		if !ok {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		fmt.Fprintln(w, val)
	})

	mux.HandleFunc("/set", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Key   string `json:"key"`
			Value string `json:"value"`
			TTL   int64  `json:"ttl"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}
		store.Set(req.Key, req.Value, time.Duration(req.TTL)*time.Second)
		if lc != nil {
			lc.Replicate("set", req.Key, req.Value, req.TTL)
		}
		w.WriteHeader(http.StatusOK)
	})

	mux.HandleFunc("/delete", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Key string `json:"key"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}
		store.Delete(req.Key)
		if lc != nil {
			lc.Replicate("delete", req.Key, "", 0)
		}
		w.WriteHeader(http.StatusOK)
	})

	log.Printf("[%s] HTTP API listening on :%s", nodeID, port)
	if err := http.ListenAndServe(":"+port, mux); err != nil {
		log.Fatalf("HTTP server failed: %v", err)
	}
}
