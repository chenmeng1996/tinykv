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
	"github.com/pingcap-incubator/tinykv/log"

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
	raftLog := &RaftLog{
		storage: c.Storage,
	}

	raft := &Raft{
		id:               c.ID,
		RaftLog:          raftLog,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
	}
	// 记录peers
	raft.votes = make(map[uint64]bool)
	for _, id := range c.peers {
		raft.votes[id] = false
	}
	return raft
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	return false
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower, StateCandidate:
		// 选举时钟转动
		r.electionElapsed += 1
		if r.electionElapsed >= r.electionTimeout {
			// 开始选举
			r.Step(pb.Message{
				MsgType: pb.MessageType_MsgHup,
			})
		}
	case StateLeader:
		// 心跳时钟转动
		r.heartbeatElapsed += 1
		if r.heartbeatElapsed >= r.heartbeatElapsed {
			// 开始发送心跳包
			msg := pb.Message{
				MsgType: pb.MessageType_MsgHeartbeat,
				Term:    r.Term,
				From:    r.id,
			}
			r.broadcast(msg)
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.Lead = lead
	r.Term = term
	r.Vote = 0
	resetVotes(r.votes)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.Term++
	// 重置投票，给自己投票
	resetVotes(r.votes)
	r.votes[r.id] = true
	if countVotes(r.votes) > len(r.votes)/2 {
		r.becomeLeader()
	}
	// 发送MessageType_MsgRequestVote给peers请求投票
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		From:    r.id,
		Term:    r.Term,
	}
	r.broadcast(msg)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	// 发起提交空entry的提议
	ent := &pb.Entry{}
	msg := pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		From:    r.id,
		Entries: []*pb.Entry{ent},
	}
	if err := r.Step(msg); err != nil {
		log.Error("become leader fail", err)
	}
	// 广播peers
	r.broadcastAppendEntries()
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			// 转换为candidate
			r.becomeCandidate()
		case pb.MessageType_MsgRequestVote:
			// 拒绝投票
			if r.Term >= m.Term || r.Vote != 0 {
				resp := pb.Message{
					MsgType: pb.MessageType_MsgRequestVoteResponse,
					From:    r.id,
					To:      m.From,
					Reject:  true,
				}
				r.msgs = append(r.msgs, resp)
				return nil
			}
			// 投票
			if r.Term < m.Term || (r.Term == m.Term && r.RaftLog.committed < m.Commit) {
				r.Vote = m.From
				resp := pb.Message{
					MsgType: pb.MessageType_MsgRequestVoteResponse,
					From:    r.id,
					To:      m.From,
					Reject:  false,
				}
				r.msgs = append(r.msgs, resp)
				return nil
			}
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		}
	case StateCandidate:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			// 转换为candidate
			r.becomeCandidate()
		case pb.MessageType_MsgRequestVote:
			return r.HandleRequestVote(&m)
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgRequestVoteResponse:
			r.votes[m.From] = !m.Reject
			if countVotes(r.votes) > len(r.votes)/2 {
				// 投票超过半数，成为leader
				r.becomeLeader()
			}
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		}
	case StateLeader:
		switch m.MsgType {
		case pb.MessageType_MsgRequestVote:
			return r.HandleRequestVote(&m)
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgPropose:
			r.propose(m)
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		}
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		if r.Term < m.Term {
			r.Term = m.Term
			r.Lead = m.From
		}
	case StateLeader, StateCandidate:
		if r.Term < m.Term {
			r.becomeFollower(m.Term, m.From)
		}
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	if r.Term > m.Term {
		return
	}
	// 重置heartbeatElapsed, 并变成follower
	r.heartbeatElapsed = 0
	switch r.State {
	case StateCandidate:
		r.becomeFollower(m.Term, m.From)
		// 更新committed index fixme
		r.RaftLog.committed = m.Commit
	case StateFollower:
		// 更新leader id
		r.Lead = m.From
		// 重置heartbeatElapsed
		r.heartbeatElapsed = 0
	}
	// heartbeat response
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From:    r.id,
		To:      m.From,
	}
	r.msgs = append(r.msgs, msg)
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

// HandleRequestVote leader和candidate处理投票请求
func (r *Raft) HandleRequestVote(m *pb.Message) error {
	if r.Term >= m.Term {
		msg := pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			From:    r.id,
			To:      m.From,
			Reject:  true,
		}
		r.msgs = append(r.msgs, msg)
		return nil
	}
	r.becomeFollower(m.Term, m.From)
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From:    r.id,
		To:      m.From,
		Reject:  false,
	}
	r.msgs = append(r.msgs, msg)
	return nil
}

func (r *Raft) broadcastAppendEntries() {
	for peerId, _ := range r.votes {
		resp := pb.Message{
			MsgType: pb.MessageType_MsgAppend,
			From:    r.id,
			To:      peerId,
			Term:    r.Term,
		}
		r.msgs = append(r.msgs, resp)
	}
}

// 广播给所有peers，除了自己
func (r *Raft) broadcast(m pb.Message) {
	for peerId, _ := range r.votes {
		if peerId == r.id {
			continue
		}
		m.To = peerId
		r.msgs = append(r.msgs, m)
	}
}

// 处理提议
func (r *Raft) propose(m pb.Message) {
	switch r.State {
	case StateFollower, StateCandidate:
		// TODO 转发给leader
	case StateLeader:
		msg := pb.Message{
			MsgType: pb.MessageType_MsgAppend,
			Entries: m.Entries,
			From:    r.id,
		}
		r.handleAppendEntries(msg)
		r.broadcast(msg)
	}
}

// 重置投票结果
func resetVotes(votes map[uint64]bool) {
	for k, _ := range votes {
		votes[k] = false
	}
}

// 统计投票结果
func countVotes(votes map[uint64]bool) int {
	count := 0
	for k, _ := range votes {
		if votes[k] == true {
			count++
		}
	}
	return count
}
