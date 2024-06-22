package higrt

import (
	"hig/higact"
)

type Variable struct {
	higact.Actor

	// The currently-held locks
	locks map[txid]lockState
	// The most recent value
	value any
	// The transactions that have written to this variable
	acceptedTransactions []tx
	// The transactions that will be required by the next value
	nextChangeRequires map[txid]tx
	// The variable's subscribers
	subscribers []higact.Address
}

type LockKind int

const (
	LockKindRead LockKind = iota
	LockKindWrite
)

type lockState any

// A client queued waiting for a lock
type lockStateQueue struct {
	kind    LockKind
	address higact.Address
}

// A lock that is held
type lockStateLock struct {
	kind    LockKind
	address higact.Address
}

// A write that is waiting for the current locks to be released
type lockStateWrite struct {
	// the list of all variables written to by this same write
	writes []higact.Address
	// the new value
	value any
}

func (rt *Runtime) NewVariable(value any) *Variable {
	return &Variable{Actor: rt.router.CreateActor(), locks: map[txid]lockState{}, nextChangeRequires: map[txid]tx{}, value: value}
}

func (v *Variable) Run() {
	for message := range v.Inbox {
		switch messageData := message.Data.(type) {
		case varLockAcquireMessage:
			oldestTxid := messageData.Txid
			for lockTxid, lock := range v.locks {
				if _, ok := lock.(lockStateLock); ok {
					if lockTxid.Lt(oldestTxid) {
						oldestTxid = lockTxid
					}
				}
			}

			if messageData.Txid == oldestTxid || (!hasWriteLock(v.locks) && !hasPendingWrite(v.locks)) {
				// allow wait: the txn wanting the lock is older or won't have to wait at all
				v.locks[messageData.Txid] = lockStateQueue{
					address: message.Sender,
					kind:    messageData.Kind,
				}
			} else {
				// disallow wait: the txn wanting the lock is younger but would have to wait
				v.Outbox <- higact.OutboundMessage{Target: message.Sender, Data: varLockFailedMessage{Txid: messageData.Txid}}
			}
		case varLockReleaseMessage:
			delete(v.locks, messageData.Txid)
		case varReadMessage:
			if l, ok := v.locks[messageData.Tx.Id]; ok {
				if l, ok := l.(lockStateLock); ok {
					v.Outbox <- higact.OutboundMessage{Target: message.Sender, Data: varReadResultMessage{Txid: messageData.Tx.Id, Value: v.value, ValueProvides: v.acceptedTransactions[max(len(v.acceptedTransactions)-1, 0):]}}
					if l.kind == LockKindRead {
						// automatically release read locks
						delete(v.locks, messageData.Tx.Id)
						// next value will have the reading transaction in its requires set
						v.nextChangeRequires[messageData.Tx.Id] = messageData.Tx
					}
				} else {
					panic("invalid messageData: attempted read when had pending write")
				}
			} else {
				panic("invalid messageData: attempted read when had no lock")
			}
		case varWriteMessage:
			if l, ok := v.locks[messageData.Tx.Id]; ok {
				if l, ok := l.(lockStateLock); ok {
					if l.kind == LockKindWrite {
						v.locks[messageData.Tx.Id] = lockStateWrite{
							writes: messageData.Tx.Writes,
							value:  messageData.Value,
						}

						for _, tx := range messageData.Requires {
							v.nextChangeRequires[tx.Id] = tx
						}
					} else {
						panic("invalid messageData: attempted to write when only had read lock")
					}
				} else {
					panic("invalid messageData: attempted write when had pending write")
				}
			} else {
				panic("invalid messageData: attempted write when had no lock")
			}
		case subscribeMessage:
			v.subscribers = append(v.subscribers, message.Sender)

			v.Outbox <- higact.OutboundMessage{Target: message.Sender, Data: subscriptionGrantedMessage{Value: v.value, Provides: v.acceptedTransactions, AncestorVariables: []higact.Address{v.Address}}}
		default:
			panic("Unexpected messageData")
		}

		if !hasWriteLock(v.locks) {
			var hasWrite bool
			if t, write, hasWrite := findPendingWrite(v.locks); hasWrite {
				if len(v.locks) == 1 {
					provides := []tx{{t, write.writes}}
					requires := make([]tx, 0, len(v.nextChangeRequires))
					for _, tx := range v.nextChangeRequires {
						requires = append(requires, tx)
					}

					for _, sub := range v.subscribers {
						v.Outbox <- higact.OutboundMessage{Target: sub, Data: changeMessage{Provides: provides, Requires: requires, Value: write.value}}
					}

					v.value = write.value
					v.acceptedTransactions = append(v.acceptedTransactions, provides...)
					v.nextChangeRequires = map[txid]tx{t: {t, write.writes}}
					v.locks = map[txid]lockState{}
					hasWrite = false
				}
			}

			for !hasWrite {
				var maxQueuedTxid txid
				var maxQueued lockStateQueue
				for queueTxid, state := range v.locks {
					if queueState, isQueue := state.(lockStateQueue); isQueue {
						if (maxQueuedTxid == txid{}) || maxQueuedTxid.Lt(queueTxid) {
							maxQueuedTxid = queueTxid
							maxQueued = queueState
						}
					}
				}

				if maxQueuedTxid != (txid{}) {
					// grant lock
					v.locks[maxQueuedTxid] = lockStateLock(maxQueued)
					v.Outbox <- higact.OutboundMessage{Target: maxQueued.address, Data: varLockGrantedMessage{Txid: maxQueuedTxid}}

					hasWrite = maxQueued.kind == LockKindWrite
				} else {
					break
				}
			}
		}
	}
}

func hasWriteLock(locks map[txid]lockState) (has bool) {
	_, _, has = findWriteLock(locks)
	return
}

func findWriteLock(locks map[txid]lockState) (txid, lockStateLock, bool) {
	for txid, l := range locks {
		if l, ok := l.(lockStateLock); ok && l.kind == LockKindWrite {
			return txid, l, true
		}
	}

	return txid{}, lockStateLock{}, false
}

func hasPendingWrite(locks map[txid]lockState) (has bool) {
	_, _, has = findPendingWrite(locks)
	return
}

func findPendingWrite(locks map[txid]lockState) (txid, lockStateWrite, bool) {
	for txid, l := range locks {
		if l, ok := l.(lockStateWrite); ok {
			return txid, l, true
		}
	}

	return txid{}, lockStateWrite{}, false
}
