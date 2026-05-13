package memflux

import (
	"context"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	pb "memflux/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type NodeState int32

const (
	StateFollower  NodeState = 0
	StateCandidate NodeState = 1
	StateLeader    NodeState = 2
)

const (
	electionTimeoutMin    = 150 * time.Millisecond
	electionTimeoutMax    = 300 * time.Millisecond
	heartbeatInterval     = 100 * time.Millisecond
	heartbeatRPCTimeout   = 50 * time.Millisecond
	requestVoteRPCTimeout = 150 * time.Millisecond
	replicateTimeout      = 500 * time.Millisecond
)

type raftPeer struct {
	addr   string
	client pb.ReplicationClient
}

type RaftNode struct {
	mu sync.Mutex

	nodeID string
	peers  []*raftPeer

	// Raft state — all protected by mu
	currentTerm  int64
	votedFor     string
	state        NodeState
	stepDownTerm int64 // highest term that triggered a step-down signal

	// sequence counter for write replication (atomic)
	sequence int64

	store *Store

	// cross-goroutine signals (buffered 1)
	heartbeatCh chan struct{} // valid heartbeat/vote received → follower resets election timer
	stepDownCh  chan struct{} // higher term seen → current state loop must exit
}

func NewRaftNode(nodeID string, peerAddrs []string, store *Store) *RaftNode {
	rn := &RaftNode{
		nodeID:      nodeID,
		store:       store,
		state:       StateFollower,
		heartbeatCh: make(chan struct{}, 1),
		stepDownCh:  make(chan struct{}, 1),
	}
	for _, addr := range peerAddrs {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("[%s] failed to connect to peer %s: %v", nodeID, addr, err)
			continue
		}
		rn.peers = append(rn.peers, &raftPeer{addr: addr, client: pb.NewReplicationClient(conn)})
	}
	return rn
}

// Run is the main Raft loop. Starts as follower and drives all state transitions.
func (rn *RaftNode) Run() {
	for {
		switch rn.getState() {
		case StateFollower:
			rn.runFollower()
		case StateCandidate:
			rn.runCandidate()
		case StateLeader:
			rn.runLeader()
		}
	}
}

func (rn *RaftNode) IsLeader() bool {
	return rn.getState() == StateLeader
}

func (rn *RaftNode) getState() NodeState {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	return rn.state
}

// ── Follower ──────────────────────────────────────────────────────────────────

func (rn *RaftNode) runFollower() {
	timer := time.NewTimer(randElectionTimeout())
	defer timer.Stop()

	for {
		select {
		case <-rn.heartbeatCh:
			resetTimer(timer, randElectionTimeout())

		case <-rn.stepDownCh:
			rn.mu.Lock()
			if rn.stepDownTerm > rn.currentTerm {
				rn.currentTerm = rn.stepDownTerm
				rn.votedFor = ""
			}
			rn.mu.Unlock()
			resetTimer(timer, randElectionTimeout())

		case <-timer.C:
			rn.mu.Lock()
			rn.state = StateCandidate
			rn.mu.Unlock()
			return
		}
	}
}

// ── Candidate ────────────────────────────────────────────────────────────────

func (rn *RaftNode) runCandidate() {
	rn.mu.Lock()
	rn.currentTerm++
	rn.votedFor = rn.nodeID
	term := rn.currentTerm
	rn.mu.Unlock()

	log.Printf("[%s] became candidate for term %d", rn.nodeID, term)

	// single-node cluster: win immediately
	if len(rn.peers) == 0 {
		rn.mu.Lock()
		rn.state = StateLeader
		rn.mu.Unlock()
		log.Printf("[%s] became leader for term %d", rn.nodeID, term)
		return
	}

	type voteResult struct {
		term    int64
		granted bool
	}
	resultCh := make(chan voteResult, len(rn.peers))

	for _, p := range rn.peers {
		go func(p *raftPeer) {
			ctx, cancel := context.WithTimeout(context.Background(), requestVoteRPCTimeout)
			defer cancel()
			resp, err := p.client.RequestVote(ctx, &pb.RequestVoteRequest{
				Term:        term,
				CandidateId: rn.nodeID,
			})
			if err != nil {
				resultCh <- voteResult{}
				return
			}
			resultCh <- voteResult{term: resp.Term, granted: resp.VoteGranted}
		}(p)
	}

	votes := 1 // self-vote
	needed := (len(rn.peers)+1)/2 + 1
	collected := 0
	timeout := time.NewTimer(randElectionTimeout())
	defer timeout.Stop()

	for collected < len(rn.peers) {
		select {
		case r := <-resultCh:
			collected++
			if r.term > term {
				log.Printf("[%s] stepping down to follower, saw higher term %d", rn.nodeID, r.term)
				rn.mu.Lock()
				rn.currentTerm = r.term
				rn.votedFor = ""
				rn.state = StateFollower
				rn.mu.Unlock()
				return
			}
			if r.granted {
				votes++
				if votes >= needed {
					rn.mu.Lock()
					rn.state = StateLeader
					rn.mu.Unlock()
					log.Printf("[%s] became leader for term %d", rn.nodeID, term)
					return
				}
			}

		case <-rn.stepDownCh:
			rn.mu.Lock()
			rn.currentTerm = rn.stepDownTerm
			rn.votedFor = ""
			rn.state = StateFollower
			rn.mu.Unlock()
			log.Printf("[%s] stepping down to follower, saw higher term %d", rn.nodeID, rn.stepDownTerm)
			return

		case <-timeout.C:
			return // no majority; Run() will call runCandidate() again with a new term
		}
	}
}

// ── Leader ────────────────────────────────────────────────────────────────────

func (rn *RaftNode) runLeader() {
	rn.mu.Lock()
	term := rn.currentTerm
	rn.mu.Unlock()

	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	rn.sendHeartbeats(term) // immediate heartbeat so followers don't time out

	for {
		select {
		case <-ticker.C:
			rn.sendHeartbeats(term)

		case <-rn.stepDownCh:
			rn.mu.Lock()
			rn.currentTerm = rn.stepDownTerm
			rn.votedFor = ""
			rn.state = StateFollower
			rn.mu.Unlock()
			log.Printf("[%s] stepping down to follower, saw higher term %d", rn.nodeID, rn.stepDownTerm)
			return
		}
	}
}

func (rn *RaftNode) sendHeartbeats(term int64) {
	for _, p := range rn.peers {
		go func(p *raftPeer) {
			ctx, cancel := context.WithTimeout(context.Background(), heartbeatRPCTimeout)
			defer cancel()
			resp, err := p.client.HeartBeat(ctx, &pb.HeartBeatRequest{
				LeaderId: rn.nodeID,
				Term:     term,
			})
			if err != nil {
				return
			}
			if resp.Term > term {
				rn.signalStepDown(resp.Term)
			}
		}(p)
	}
}

// ── gRPC handler callbacks ────────────────────────────────────────────────────

// HandleHeartbeat is called by server.go when a HeartBeat RPC arrives.
// Returns the node's current term for inclusion in the response.
func (rn *RaftNode) HandleHeartbeat(term int64, leaderID string) (currentTerm int64, reject bool) {
	rn.mu.Lock()

	if term < rn.currentTerm {
		ct := rn.currentTerm
		rn.mu.Unlock()
		return ct, true
	}

	wasNonFollower := rn.state != StateFollower
	if term > rn.currentTerm {
		log.Printf("[%s] stepping down to follower, saw higher term %d", rn.nodeID, term)
		rn.currentTerm = term
		rn.votedFor = ""
	}
	rn.state = StateFollower
	ct := rn.currentTerm
	rn.mu.Unlock()

	rn.signalHeartbeat()
	if wasNonFollower {
		rn.signalStepDown(term)
	}

	return ct, false
}

// HandleRequestVote is called by server.go when a RequestVote RPC arrives.
func (rn *RaftNode) HandleRequestVote(term int64, candidateID string) (currentTerm int64, granted bool) {
	rn.mu.Lock()

	if term < rn.currentTerm {
		ct := rn.currentTerm
		rn.mu.Unlock()
		return ct, false
	}

	stepDown := false
	if term > rn.currentTerm {
		rn.currentTerm = term
		rn.votedFor = ""
		stepDown = rn.state != StateFollower
		rn.state = StateFollower
	}

	grant := rn.votedFor == "" || rn.votedFor == candidateID
	if grant {
		rn.votedFor = candidateID
	}
	ct := rn.currentTerm
	rn.mu.Unlock()

	if grant {
		log.Printf("[%s] granted vote to %s for term %d", rn.nodeID, candidateID, term)
		rn.signalHeartbeat() // reset election timer when granting vote
	}
	if stepDown {
		log.Printf("[%s] stepping down to follower, saw higher term %d", rn.nodeID, term)
		rn.signalStepDown(term)
	}

	return ct, grant
}

// ── Replication (leader only) ─────────────────────────────────────────────────

// Replicate fans a write out to all peers. Called by HTTP handlers after
// applying the write to the local store.
func (rn *RaftNode) Replicate(operation, key, value string, ttlSeconds int64) {
	req := &pb.ReplicateRequest{
		Key:        key,
		Value:      value,
		TtlSeconds: ttlSeconds,
		Sequence:   atomic.AddInt64(&rn.sequence, 1),
		Operation:  operation,
	}
	for _, p := range rn.peers {
		go func(p *raftPeer) {
			for i := 0; i < 3; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), replicateTimeout)
				resp, err := p.client.Replicate(ctx, req)
				cancel()
				if err == nil && resp.Success {
					return
				}
				log.Printf("[%s] replicate to %s attempt %d: %v", rn.nodeID, p.addr, i+1, err)
			}
		}(p)
	}
}

// ── helpers ───────────────────────────────────────────────────────────────────

func randElectionTimeout() time.Duration {
	span := int64(electionTimeoutMax - electionTimeoutMin)
	return electionTimeoutMin + time.Duration(rand.Int63n(span))
}

func resetTimer(t *time.Timer, d time.Duration) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
	t.Reset(d)
}

// signalStepDown records the highest term seen and wakes up any blocking state loop.
func (rn *RaftNode) signalStepDown(term int64) {
	rn.mu.Lock()
	if term > rn.stepDownTerm {
		rn.stepDownTerm = term
	}
	rn.mu.Unlock()
	select {
	case rn.stepDownCh <- struct{}{}:
	default:
	}
}

// signalHeartbeat wakes up the follower loop to reset its election timer.
func (rn *RaftNode) signalHeartbeat() {
	select {
	case rn.heartbeatCh <- struct{}{}:
	default:
	}
}
