package drpdelta

func (v *variable) Run() {
	for inMsg := range v.inbox {
		switch inContent := inMsg.Content.(type) {
		case LockMessage:
			if _, _, found := findWriteLock(v.locks); found {
				var youngestTxid Txid
				for txid := range v.locks {
					if (youngestTxid == Txid{}) || txid.Lt(youngestTxid) {
						youngestTxid = txid
					}
				}

				if inContent.txid.Lt(youngestTxid) {
					// allow wait: the txn wanting the lock is older (has smaller timestamp)
					v.queue[inContent.txid] = struct {
						address Address
						kind    LockKind
					}{
						address: inMsg.Sender,
						kind:    inContent.kind,
					}
				} else {
					// disallow wait: the txn wanting the lock is younger (has larger timestamp)
					v.outbox <- OutboundMessage{Target: inMsg.Sender, Content: LockAbortMessage{txid: inContent.txid}}
				}
			} else if _, _, found := findPendingWrite(v.locks); found {
				var youngestReadTxid Txid
				for txid, lock := range v.locks {
					if _, ok := lock.(lockStateLock); ok {
						if (youngestReadTxid == Txid{}) || txid.Lt(youngestReadTxid) {
							youngestReadTxid = txid
						}
					}
				}

				if (youngestReadTxid == Txid{}) || inContent.txid.Lt(youngestReadTxid) {
					// allow wait: the txn wanting the lock is older (has smaller timestamp)
					v.queue[inContent.txid] = struct {
						address Address
						kind    LockKind
					}{
						address: inMsg.Sender,
						kind:    inContent.kind,
					}
				} else {
					// disallow wait: the txn wanting the lock is younger (has larger timestamp)
					v.outbox <- OutboundMessage{Target: inMsg.Sender, Content: LockAbortMessage{txid: inContent.txid}}
				}
			} else {
				// add to the queue to be added in tick()
				v.queue[inContent.txid] = struct {
					address Address
					kind    LockKind
				}{
					address: inMsg.Sender,
					kind:    inContent.kind,
				}
			}
		case LockAbortMessage:
			delete(v.locks, inContent.txid)
			delete(v.queue, inContent.txid)
		case ReadMessage:
			if l, ok := v.locks[inContent.tx.id]; ok {
				if l, ok := l.(lockStateLock); ok {
					v.outbox <- OutboundMessage{Target: inMsg.Sender, Content: ReadResultMessage{txid: inContent.tx.id, value: v.value, valueTx: v.valueTx}}
					if l.kind == LockKindRead {
						delete(v.locks, inContent.tx.id)
					}

					if l.kind == LockKindRead {
						v.preds = append(v.preds, inContent.tx)
					}
				} else {
					panic("invalid message: attempted read when had pending write")
				}
			} else {
				panic("invalid message: attempted read when had no lock")
			}
		case WriteMessage:
			if l, ok := v.locks[inContent.tx.id]; ok {
				if l, ok := l.(lockStateLock); ok {
					if l.kind == LockKindWrite {
						v.locks[inContent.tx.id] = lockStateWrite{
							writes: inContent.tx.writes,
							value:  inContent.value,
						}

						v.preds = append(v.preds, inContent.preds...)
					} else {
						panic("invalid message: attempted to write when only had read lock")
					}
				} else {
					panic("invalid message: attempted write when had pending write")
				}
			} else {
				panic("invalid message: attempted write when had no lock")
			}
		default:
			panic("Unexpected message")
		}

		v.tick()
	}
}

func (v *variable) tick() {
	if !hasWriteLock(v.locks) {
		noWrite := false
		if t, write, ok := findPendingWrite(v.locks); ok {
			if len(v.locks) == 1 {
				// finish the pending write

				for _, sub := range v.subscribers {
					v.outbox <- OutboundMessage{Target: sub, Content: ChangeMessage{txs: []Tx{{id: t, writes: write.writes}}, preds: v.preds, value: write.value}}
				}

				v.value = write.value
				v.valueTx = Tx{id: t, writes: write.writes}
				v.preds = []Tx{v.valueTx}
				v.locks = map[Txid]lockState{}
				noWrite = true
			}
		} else {
			noWrite = true
		}

		if noWrite && len(v.queue) > 0 {
			var maxTxid Txid
			for txid := range v.queue {
				if (maxTxid == Txid{}) || maxTxid.Lt(txid) {
					maxTxid = txid
				}
			}

			if maxTxid != (Txid{}) {
				// grant lock
				v.locks[maxTxid] = lockStateLock{kind: v.queue[maxTxid].kind, address: v.queue[maxTxid].address}
				v.outbox <- OutboundMessage{Target: v.queue[maxTxid].address, Content: LockGrantedMessage{txid: maxTxid}}
				delete(v.queue, maxTxid)
			}
		}
	}
}

func hasWriteLock(locks map[Txid]lockState) bool {
	for _, l := range locks {
		if l, ok := l.(lockStateLock); ok && l.kind == LockKindWrite {
			return true
		}
	}

	return false
}

func findWriteLock(locks map[Txid]lockState) (Txid, lockStateLock, bool) {
	for txid, l := range locks {
		if l, ok := l.(lockStateLock); ok && l.kind == LockKindWrite {
			return txid, l, true
		}
	}

	return Txid{}, lockStateLock{}, false
}

func findPendingWrite(locks map[Txid]lockState) (Txid, lockStateWrite, bool) {
	for txid, l := range locks {
		if l, ok := l.(lockStateWrite); ok {
			return txid, l, true
		}
	}

	return Txid{}, lockStateWrite{}, false
}

func (o *orchestrator) NewVariable(address Address, value any) *variable {
	inbox, outbox := o.RegisterActor(address)

	return &variable{
		address:     address,
		inbox:       inbox,
		outbox:      outbox,
		value:       value,
		valueTx:     Tx{id: Txid{}, writes: []Address{}},
		subscribers: []Address{},
		queue: map[Txid]struct {
			address Address
			kind    LockKind
		}{},
		locks: map[Txid]lockState{},
		preds: []Tx{{id: Txid{}, writes: []Address{}}},
	}
}

func (v *variable) Subscribe(who Address) (any, map[Address]struct{}) {
	v.subscribers = append(v.subscribers, who)

	return v.value, map[Address]struct{}{(v.address): {}}
}

func (v *variable) Address() Address {
	return v.address
}

type variable struct {
	address     Address
	inbox       <-chan InboundMessage
	outbox      chan<- OutboundMessage
	value       any
	valueTx     Tx
	subscribers []Address
	queue       map[Txid]struct {
		address Address
		kind    LockKind
	}
	locks map[Txid]lockState
	preds []Tx
}

type LockKind int

const (
	LockKindRead LockKind = iota
	LockKindWrite
)

type lockState any

type lockStateLock struct {
	kind    LockKind
	address Address
}

type lockStateWrite struct {
	// the list of all variables written to by this same write
	writes []Address
	// the new value
	value any
}
