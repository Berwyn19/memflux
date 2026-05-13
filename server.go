package memflux

import (
	"context"
	pb "memflux/proto"
	"time"
)

type ReplicationServer struct {
	pb.UnimplementedReplicationServer
	store    *Store
	nodeID   string
	raftNode *RaftNode
}

func NewReplicationServer(store *Store, nodeID string, raftNode *RaftNode) *ReplicationServer {
	return &ReplicationServer{
		store:    store,
		nodeID:   nodeID,
		raftNode: raftNode,
	}
}

func (s *ReplicationServer) Replicate(ctx context.Context, req *pb.ReplicateRequest) (*pb.ReplicateResponse, error) {
	if req.Operation == "set" {
		s.store.Set(req.Key, req.Value, time.Duration(req.TtlSeconds)*time.Second)
	} else if req.Operation == "delete" {
		s.store.Delete(req.Key)
	} else {
		return &pb.ReplicateResponse{Success: false, Error: "unknown operation"}, nil
	}
	return &pb.ReplicateResponse{Success: true}, nil
}

func (s *ReplicationServer) HeartBeat(ctx context.Context, req *pb.HeartBeatRequest) (*pb.HeartBeatResponse, error) {
	term, reject := s.raftNode.HandleHeartbeat(req.Term, req.LeaderId)
	return &pb.HeartBeatResponse{Alive: !reject, Term: term}, nil
}

func (s *ReplicationServer) RequestVote(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	term, granted := s.raftNode.HandleRequestVote(req.Term, req.CandidateId)
	return &pb.RequestVoteResponse{Term: term, VoteGranted: granted}, nil
}
