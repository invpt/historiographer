package drpdelta

func (v *variable) server() {
	for inMsg := range v.inbox {
		switch inContent := inMsg.Content.(type) {
		case QueueMessage:
			if _, ok := v.queue[inContent.txid]; !ok {
				v.queue[inContent.txid] = struct {
					state queueState
					Address
					Txid
					kind lockKind
				}{
					state:   pending,
					Address: inMsg.Sender,
				}
			}
		case LockMessage:
			if qState, ok := v.queue[inContent.txid]; ok && qState.state == ready {
				qState.kind = inContent.kind
				qState.state = requested
				v.queue[inContent.txid] = qState
			}
		case ReadMessage:
			if l, ok := v.locks[inContent.tx.id]; ok {
				if l, ok := l.(lockStateLock); ok {
					v.outbox <- OutboundMessage{Target: inMsg.Sender, Content: ReadResultMessage{txid: inContent.tx.id, value: v.value, valueTx: v.valueTx}}
					if l.kind == read {
						delete(v.locks, inContent.tx.id)
					}

					v.preds = append(v.preds, inContent.tx)
				} else {
					panic("invalid message: attempted read when had pending write")
				}
			} else {
				panic("invalid message: attempted read when had no lock")
			}
		case WriteMessage:
			if l, ok := v.locks[inContent.tx.id]; ok {
				if l, ok := l.(lockStateLock); ok {
					if l.kind == write {
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
		}

		v.tick()
	}
}

func (v *variable) tick() {
	if !hasWriteLock(v.locks) {
		for txid, qState := range v.queue {
			if qState.state == pending {
				qState.state = ready
				v.queue[txid] = qState
				v.outbox <- OutboundMessage{Target: qState.Address, Content: QueueGrantedMessage{txid: txid}}
			}
		}

		if t, write, ok := findPendingWrite(v.locks); ok {
			if len(v.locks) == 1 {
				for _, sub := range v.subscribers {
					v.outbox <- OutboundMessage{Target: sub, Content: ChangeMessage{txs: []Tx{{id: t, writes: write.writes}}, preds: v.preds, value: write.value}}
				}

				v.value = write.value
				v.valueTx = Tx{id: t, writes: write.writes}
				v.preds = []Tx{v.valueTx}
			}
		} else if len(v.locks) > 0 {
			var minTxid Txid
			for txid := range v.queue {
				if txid < minTxid {
					minTxid = txid
				}
			}

			if v.queue[minTxid].state == requested {
				// grant lock
				v.outbox <- OutboundMessage{Target: v.queue[minTxid].Address, Content: LockGrantedMessage{txid: minTxid, kind: v.queue[minTxid].kind}}
			}
		}
	}
}

func hasWriteLock(locks map[Txid]lockState) bool {
	for _, l := range locks {
		if l, ok := l.(lockStateLock); ok && l.kind == write {
			return true
		}
	}

	return false
}

func findPendingWrite(locks map[Txid]lockState) (Txid, lockStateWrite, bool) {
	for txid, l := range locks {
		if l, ok := l.(lockStateWrite); ok {
			return txid, l, true
		}
	}

	return Txid(""), lockStateWrite{}, false
}

func NewVariable(address Address, value any) variable {
	return variable{
		Address:     address,
		value:       value,
		subscribers: []Address{},
		queue: map[Txid]struct {
			state queueState
			Address
			Txid
			kind lockKind
		}{},
		locks: map[Txid]lockState{},
		preds: []Tx{{id: "init", writes: []Address{}}},
	}
}

func (v *variable) Subscribe(who Address) {
	v.subscribers = append(v.subscribers, who)

	v.outbox <- OutboundMessage{Target: who, Content: ChangeMessage{txs: []Tx{v.valueTx}, preds: []Tx{}, value: v.value}}
}

type variable struct {
	Address     Address
	inbox       <-chan InboundMessage
	outbox      chan<- OutboundMessage
	value       any
	valueTx     Tx
	subscribers []Address
	queue       map[Txid]struct {
		state queueState
		Address
		Txid
		kind lockKind
	}
	locks map[Txid]lockState
	preds []Tx
}

type queueState int

const (
	pending queueState = iota
	ready
	requested
)

type lockKind int

const (
	read lockKind = iota
	write
)

type lockState any

type lockStateLock struct {
	kind    lockKind
	address Address
}

type lockStateWrite struct {
	// the list of all variables written to by this same write
	writes []Address
	// the new value
	value any
}
