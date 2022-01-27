// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"math/rand"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// election interval based on electionTimeout
	// should be random between electionTimeout and 3/2 electionTimeout
	electionInterval int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	s := c.Storage
	r := &Raft{
		id:               c.ID,
		Prs:              make(map[uint64]*Progress),
		votes:            make(map[uint64]bool),
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		msgs:             make([]pb.Message, 0),
		RaftLog:          newLog(s),
		State:            StateFollower,
	}
	hardState, confState, _ := s.InitialState()
	r.Vote, r.Term, r.RaftLog.committed = hardState.GetVote(), hardState.GetTerm(), hardState.GetCommit()
	if c.Applied > 0 {
		r.RaftLog.applied = c.Applied
	}
	// nodes config
	nodes := make([]uint64, 0)
	if c.peers == nil {
		// restart a node: try to get nodes info from confState
		nodes = confState.Nodes
	} else {
		// first time to start a node
		nodes = c.peers
	}
	for _, n := range nodes {
		r.votes[n] = false
		//initialize each node's log replication progress
		r.Prs[n] = &Progress{
			Match: 0,
			Next:  1,
		}
	}
	// random election interval
	r.electionInterval = c.ElectionTick + rand.Intn(c.ElectionTick)
	return r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	if r.Prs[to].Next > r.RaftLog.LastIndex() {
		return false
	}
	msg := pb.Message{MsgType: pb.MessageType_MsgAppend, To: to, From: r.id, Term: r.Term, Entries: make([]*pb.Entry, 0)}
	msg.LogTerm, _ = r.RaftLog.Term(r.Prs[to].Next - 1)
	for i := r.Prs[to].Next; i <= r.RaftLog.LastIndex(); i++ {
		msg.Entries = append(msg.Entries, &r.RaftLog.entries[r.RaftLog.sliceIndex(i)])
	}
	msg.Index, msg.Commit = r.Prs[to].Next, r.RaftLog.committed
	r.msgs = append(r.msgs, msg)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) sendVoteReq(to uint64) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		To:      to,
		From:    r.id,
		Term:    r.Term,
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) bcastHeartbeat() {
	r.heartbeatElapsed = 0
	for n := range r.Prs {
		if n == r.id {
			continue
		}
		r.sendHeartbeat(n)
	}
}

func (r *Raft) bcastVoteReq() {
	for n := range r.Prs {
		if n == r.id {
			continue
		}
		r.sendVoteReq(n)
	}
}

func (r *Raft) bcastAppend() {
	for n := range r.Prs {
		if n == r.id {
			continue
		}
		r.sendAppend(n)
	}
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		fallthrough
	case StateCandidate:
		if r.electionElapsed++; r.electionElapsed >= r.electionInterval {
			r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
		}
	case StateLeader:
		if r.heartbeatElapsed++; r.heartbeatElapsed >= r.heartbeatTimeout {
			r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat})
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.electionElapsed = 0
	r.electionInterval = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.Term = term
	r.Lead = lead
	r.Vote = None
	r.State = StateFollower
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.electionElapsed = 0
	r.electionInterval = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.State = StateCandidate
	r.Lead = None
	r.Term++
	// start a new election and vote for self
	for n := range r.votes {
		r.votes[n] = false
	}
	r.votes[r.id] = true
	r.Vote = r.id
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.heartbeatElapsed = 0
	r.State = StateLeader
	r.Step(pb.Message{MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{}}})
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		return r.stepFollower(m)
	case StateCandidate:
		return r.stepCandidate(m)
	case StateLeader:
		return r.stepLeader(m)
	}
	return nil
}

func (r *Raft) stepFollower(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.handleHup()
	case pb.MessageType_MsgBeat:
	case pb.MessageType_MsgPropose:
		m.To = r.Lead
		r.msgs = append(r.msgs, m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
	case pb.MessageType_MsgSnapshot:
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
	case pb.MessageType_MsgTransferLeader:
	case pb.MessageType_MsgTimeoutNow:
	}
	return nil
}

func (r *Raft) stepCandidate(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.handleHup()
	case pb.MessageType_MsgBeat:
	case pb.MessageType_MsgPropose:
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.votes[m.From] = !m.Reject
		agrNum := 0 // 赞同个数
		for _, v := range r.votes {
			if v == true {
				agrNum++
			}
		}
		if agrNum > (len(r.votes))/2 {
			r.becomeLeader()
		}
	case pb.MessageType_MsgSnapshot:
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
	case pb.MessageType_MsgTransferLeader:
	case pb.MessageType_MsgTimeoutNow:
	}
	return nil
}

func (r *Raft) stepLeader(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
	case pb.MessageType_MsgBeat:
		r.bcastHeartbeat()
	case pb.MessageType_MsgPropose:
		for i := range m.Entries {
			m.Entries[i].Term = r.Term
			m.Entries[i].Index = r.RaftLog.LastIndex() + uint64(1+i)
		}
		r.RaftLog.appendEntries(m.Entries)
		r.bcastAppend()
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
	case pb.MessageType_MsgSnapshot:
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
	case pb.MessageType_MsgTransferLeader:
	case pb.MessageType_MsgTimeoutNow:
	}
	return nil
}

// handleHup handle Hup RPC request
func (r *Raft) handleHup() {
	r.becomeCandidate()
	if len(r.votes) == 1 {
		r.becomeLeader()
		return
	}
	r.bcastVoteReq()
}

// handleRequestVote handle RequestVote RPC request
func (r *Raft) handleRequestVote(m pb.Message) {
	res := pb.Message{MsgType: pb.MessageType_MsgRequestVoteResponse, To: m.From, From: r.id}
	switch r.State {
	case StateFollower:
		if m.Term > r.Term {
			r.Term = m.Term
			r.Vote = m.From
		} else if m.Term == r.Term {
			if r.Vote != None {
				if r.Vote != m.From {
					res.Reject = true
				}
			} else {
				if m.Commit >= r.RaftLog.committed {
					r.Vote = m.From
				} else {
					res.Reject = true
				}
			}
		} else {
			res.Reject = true
		}
	case StateCandidate:
		fallthrough
	case StateLeader:
		if m.Term > r.Term {
			r.State = StateFollower
			r.Vote = m.From
			r.Term = m.Term
			r.electionElapsed = 0
		} else {
			res.Reject = true
		}
	}
	res.Term = r.Term
	res.Commit = r.RaftLog.committed
	r.msgs = append(r.msgs, res)
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	response := pb.Message{MsgType: pb.MessageType_MsgAppendResponse, To: m.From, From: r.id}
	if r.Term > m.Term {
		response.Reject = true
	} else {
		r.becomeFollower(m.Term, m.From)
		if m.Entries == nil || len(m.Entries) == 0 {
			// 空日志处理
			response.Reject = false
			r.msgs = append(r.msgs, response)
			return
		} else {
			// 检查是否能够找到匹配的index
			if r.RaftLog.LastIndex()+1 < m.Index {
				response.Reject = true
			} else {
				// 检查日志历史是否匹配
				prevTerm, _ := r.RaftLog.Term(m.Index - 1)
				if prevTerm != m.LogTerm {
					// 日志历史不匹配
					response.Reject = true
				} else {
					// 搜索最小不匹配日志索引号
					i := uint64(0)
					for ; i+m.Index <= r.RaftLog.LastIndex() && i < uint64(len(m.Entries)); i++ {
						term, _ := r.RaftLog.Term(uint64(i + m.Index))
						if m.Entries[i].Term != term {
							break
						}
					}
					if i+m.Index != r.RaftLog.LastIndex()+1 {
						// 存在不匹配日志段，截断丢弃
						r.RaftLog.entries = r.RaftLog.entries[:r.RaftLog.sliceIndex(i+m.Index)]
					}
					if i < uint64(len(m.Entries)) {
						// 有新日志则追加新的日志
						r.RaftLog.appendEntries(m.Entries[i:])
					}
					r.RaftLog.committed = max(r.RaftLog.committed, min(r.RaftLog.LastIndex(), m.Commit))
				}
			}
		}
	}
	response.Commit = r.RaftLog.committed
	response.Term = r.Term
	r.msgs = append(r.msgs, response)
	return
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	if m.Term > r.Term {
		r.becomeFollower(m.Term, m.From)
		r.RaftLog.committed = max(r.RaftLog.committed, min(r.RaftLog.LastIndex(), m.Commit))
		r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgHeartbeatResponse, From: r.id, To: m.From})
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

func (r *Raft) appendEntries(entries []*pb.Entry) {
	for i, _ := range entries {
		entries[i].Term = r.Term
	}
	r.RaftLog.appendEntries(entries)
}
