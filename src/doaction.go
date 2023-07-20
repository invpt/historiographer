package drpdelta

import (
	"github.com/google/uuid"
)

func (o *orchestrator) DoAction(action Action) {
	address := Address("action-" + uuid.New().String())
	inbox, outbox := o.RegisterActor(address)

Try:
	for {
		txid := NewTxid(address)

		// make a list of all the things we write to for later use
		writes := []Address{}
		for name, kind := range action.Targets {
			if kind == LockKindWrite {
				writes = append(writes, name)
			}
		}

		// request all locks
		for name, kind := range action.Targets {
			outbox <- OutboundMessage{Target: name, Content: LockMessage{txid: txid, kind: kind}}
		}
		// wait to be granted all locks
		lockResponseCount := 0
	WaitLocks:
		for inMsg := range inbox {
			switch inContent := inMsg.Content.(type) {
			case LockGrantedMessage:
				if inContent.txid == txid {
					lockResponseCount += 1

					if lockResponseCount == len(action.Targets) {
						break WaitLocks
					}
				}
			case LockAbortMessage:
				if inContent.txid == txid {
					for address := range action.Targets {
						if address == inMsg.Sender {
							continue
						}

						outbox <- OutboundMessage{Target: address, Content: LockAbortMessage{txid: txid}}
					}

					continue Try
				}
			default:
				panic("Unexpected message")
			}
		}

		// request all reads
		for name := range action.Targets {
			outbox <- OutboundMessage{Target: name, Content: ReadMessage{tx: Tx{id: txid, writes: writes}}}
		}
		// wait to be granted all reads
		values := map[Address]any{}
		preds := []Tx{}
		for inMsg := range inbox {
			if inContent, ok := inMsg.Content.(ReadResultMessage); ok {
				values[inMsg.Sender] = inContent.value
				preds = append(preds, inContent.valueTx)

				if len(values) == len(action.Targets) {
					break
				}
			}
		}

		// do all writes
		for name, value := range action.F(values) {
			if kind, ok := action.Targets[name]; !ok || kind != LockKindWrite {
				panic("invariant broken: action tries to write to variable it doesn't have a write lock for")
			}

			outbox <- OutboundMessage{Target: name, Content: WriteMessage{tx: Tx{id: txid, writes: writes}, preds: preds, value: value}}
		}

		break
	}
}

type Action struct {
	Targets map[Address]LockKind
	F       func(map[Address]any) map[Address]any
}
