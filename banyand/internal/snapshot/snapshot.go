// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Package snapshot provides generic transaction coordination for snapshot-based systems.
// It enables atomic updates across multiple heterogeneous snapshot managers using Go generics.
package snapshot

import (
	"reflect"
	"sync"

	"github.com/apache/skywalking-banyandb/pkg/pool"
)

// Snapshot is a type constraint for snapshot types that support reference counting.
// Any type implementing this interface can participate in atomic transactions.
type Snapshot interface {
	// IncRef increments the reference count.
	IncRef()
	// DecRef decrements the reference count and releases resources when zero.
	DecRef()
}

// Manager is a generic interface for managing snapshots of type S.
// Both trace and sidx implement this interface for their respective snapshot types.
type Manager[S Snapshot] interface {
	// CurrentSnapshot returns the current snapshot with incremented ref count.
	// Returns nil if no snapshot exists.
	CurrentSnapshot() S
	// ReplaceSnapshot atomically replaces the current snapshot with next.
	// The old snapshot's DecRef is called automatically.
	ReplaceSnapshot(next S)
}

// Transition represents a prepared but uncommitted snapshot change.
// It holds both the current and next snapshots, allowing atomic commit or rollback.
type Transition[S Snapshot] struct {
	manager   Manager[S]
	current   S
	next      S
	committed bool
}

// getTransitionPool returns or creates a pool for the given snapshot type.
func getTransitionPool[S Snapshot]() *pool.Synced[any] {
	var zero S
	typ := reflect.TypeOf(zero)

	if p, ok := transitionPools.Load(typ); ok {
		return p.(*pool.Synced[any])
	}

	// Create new pool for this type
	poolName := "snapshot.Transition[" + typ.String() + "]"
	p := pool.Register[any](poolName)
	actual, _ := transitionPools.LoadOrStore(typ, p)
	return actual.(*pool.Synced[any])
}

// NewTransition creates a transition by preparing the next snapshot from the pool.
// The prepareNext function receives the current snapshot and returns the next.
func NewTransition[S Snapshot](manager Manager[S], prepareNext func(current S) S) *Transition[S] {
	p := getTransitionPool[S]()

	var t *Transition[S]
	if pooled := p.Get(); pooled != nil {
		t = pooled.(*Transition[S])
	} else {
		t = &Transition[S]{}
	}

	current := manager.CurrentSnapshot()
	next := prepareNext(current)

	t.manager = manager
	t.current = current
	t.next = next
	t.committed = false

	return t
}

// Commit commits the transition, replacing the current snapshot with next.
// ReplaceSnapshot satisfies the Manager contract by calling DecRef on the old snapshot.
func (t *Transition[S]) Commit() {
	if t.committed {
		return
	}
	t.committed = true
	t.manager.ReplaceSnapshot(t.next)
}

// Rollback discards the prepared next snapshot without committing.
func (t *Transition[S]) Rollback() {
	if t.committed {
		return
	}
	// Release the prepared next snapshot
	// Use reflection to check if underlying value is nil (Go interface quirk)
	if !reflect.ValueOf(t.next).IsNil() {
		t.next.DecRef()
	}
	// Release reference to current (acquired in NewTransition)
	if !reflect.ValueOf(t.current).IsNil() {
		t.current.DecRef()
	}
}

// Release returns the transition to the pool for reuse.
// This should be called after Commit or Rollback to reduce allocations.
func (t *Transition[S]) Release() {
	t.reset()
	p := getTransitionPool[S]()
	p.Put(any(t))
}

// reset clears the transition state for reuse.
// When the transition was committed, the ref acquired in NewTransition (t.current) was not
// decremented by ReplaceSnapshot (which only decrements the manager's ref), so we release it here.
func (t *Transition[S]) reset() {
	var zero S
	if t.committed && !reflect.ValueOf(t.current).IsNil() {
		t.current.DecRef()
	}
	t.manager = nil
	t.current = zero
	t.next = zero
	t.committed = false
}

var (
	transactionPool = pool.Register[*Transaction]("snapshot.Transaction")
	transitionPools sync.Map // map[reflect.Type]*pool.Synced[any]
)

// Transaction coordinates atomic commits across multiple heterogeneous transitions.
// It uses type erasure via function closures to handle different snapshot types.
type Transaction struct {
	commits   []func()
	rollbacks []func()
	mu        sync.Mutex
	finalized bool
}

// NewTransaction creates a new empty transaction from the pool.
func NewTransaction() *Transaction {
	txn := transactionPool.Get()
	if txn == nil {
		txn = &Transaction{}
	}
	return txn
}

// Release returns the transaction to the pool for reuse.
// This should be called after Commit or Rollback to reduce allocations.
func (txn *Transaction) Release() {
	txn.reset()
	transactionPool.Put(txn)
}

// reset clears the transaction state for reuse.
func (txn *Transaction) reset() {
	txn.commits = txn.commits[:0]
	txn.rollbacks = txn.rollbacks[:0]
	txn.finalized = false
}

// AddTransition adds a typed transition to the transaction.
// This is a generic function (not a method) because Go doesn't support generic methods.
func AddTransition[S Snapshot](txn *Transaction, transition *Transition[S]) {
	txn.commits = append(txn.commits, transition.Commit)
	txn.rollbacks = append(txn.rollbacks, transition.Rollback)
}

// Commit atomically commits all transitions in the transaction.
// All commits happen under a single lock to ensure atomicity.
func (txn *Transaction) Commit() {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.finalized {
		return
	}
	txn.finalized = true

	for _, commit := range txn.commits {
		commit()
	}
}

// Rollback discards all prepared transitions without committing.
// Rollbacks are executed in reverse order (LIFO).
func (txn *Transaction) Rollback() {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.finalized {
		return
	}
	txn.finalized = true

	for i := len(txn.rollbacks) - 1; i >= 0; i-- {
		txn.rollbacks[i]()
	}
}
