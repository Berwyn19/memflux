package main

import (
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
)

func main() {
	port := flag.String("port", "8001", "gRPC port to listen on")
	httpPort := flag.String("http-port", "", "HTTP API port (default: gRPC port + 1000)")
	peers := flag.String("peers", "", "comma-separated gRPC addresses of all other nodes")
	flag.Parse()

	if *httpPort == "" {
		p, _ := strconv.Atoi(*port)
		*httpPort = strconv.Itoa(p + 1000)
	}

	nodeID := "node-" + *port

	var peerList []string
	if *peers != "" {
		peerList = strings.Split(*peers, ",")
	}

	store := memflux.NewStore()
	raftNode := memflux.NewRaftNode(nodeID, peerList, store)
	server := memflux.NewReplicationServer(store, nodeID, raftNode)

	lis, err := net.Listen("tcp", ":"+*port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterReplicationServer(grpcServer, server)
	go func() {
		log.Printf("[%s] gRPC listening on :%s", nodeID, *port)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("gRPC failed: %v", err)
		}
	}()

	go raftNode.Run()

	go startHTTP(nodeID, *httpPort, store, raftNode)

	select {}
}

func startHTTP(nodeID, port string, store *memflux.Store, rn *memflux.RaftNode) {
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
		if !rn.IsLeader() {
			http.Error(w, "not leader", http.StatusServiceUnavailable)
			return
		}
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
		rn.Replicate("set", req.Key, req.Value, req.TTL)
		w.WriteHeader(http.StatusOK)
	})

	mux.HandleFunc("/delete", func(w http.ResponseWriter, r *http.Request) {
		if !rn.IsLeader() {
			http.Error(w, "not leader", http.StatusServiceUnavailable)
			return
		}
		var req struct {
			Key string `json:"key"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}
		store.Delete(req.Key)
		rn.Replicate("delete", req.Key, "", 0)
		w.WriteHeader(http.StatusOK)
	})

	log.Printf("[%s] HTTP listening on :%s", nodeID, port)
	if err := http.ListenAndServe(":"+port, mux); err != nil {
		log.Fatalf("HTTP failed: %v", err)
	}
}
