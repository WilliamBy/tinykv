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
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	raftLog := &RaftLog{storage: storage}
	hs, _, _ := storage.InitialState()
	raftLog.committed = hs.Commit
	raftLog.stabled, _ = storage.LastIndex()
	// extract entries from storage
	first, _ := storage.FirstIndex()
	last, _ := storage.LastIndex()
	s_ents, _ := storage.Entries(first, last+1)
	raftLog.entries = s_ents
	return raftLog
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	firstUnstabledIdx := l.sliceIndex(l.stabled + 1)
	if firstUnstabledIdx == -1 {
		return []pb.Entry{}
	}
	return l.entries[firstUnstabledIdx:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	ents = make([]pb.Entry, 0)
	si4apply, si4commit := l.sliceIndex(l.applied), l.sliceIndex(l.committed)
	if si4commit != -1 && si4apply != -1 {
		ents = l.entries[si4apply+1 : si4commit+1]
	} else if l.applied == 0 {
		ents = l.entries[:si4commit+1]
	}
	return
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		return 0
	}
	return l.entries[len(l.entries)-1].Index
}

// FirstIndex return the first index of the log entries
func (l *RaftLog) FirstIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		return 0
	}
	return l.entries[0].Index
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if i == 0 {
		return 0, nil
	}
	idx := l.sliceIndex(i)
	if idx == -1 {
		return 0, ErrUnavailable
	}
	return l.entries[idx].Term, nil
}

func (l *RaftLog) appendEntries(ents []*pb.Entry) {
	for i, _ := range ents {
		ents[i].Index = l.LastIndex() + 1
		l.entries = append(l.entries, *ents[i])
	}
}

// transfer the index of log to the index of slice
func (l *RaftLog) sliceIndex(logIndex uint64) int {
	if logIndex < l.FirstIndex() || logIndex > l.LastIndex() {
		return -1
	}
	return int(logIndex - l.FirstIndex())
}
