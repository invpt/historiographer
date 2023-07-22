package drpdelta

import (
	"time"

	"github.com/google/uuid"
)

func (o *orchestrator) DoAction(action Action) {
	if len(action.Targets) == 0 {
		panic("Attempted action with 0 targets!")
	}

	address := Address("action-" + uuid.New().String())
	inbox, outbox := o.RegisterActor(address)

	txid := NewTxid(address)

Try:
	for {

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
		aborters := map[Address]struct{}{}
		lockResponseCount := 0
	WaitLocks:
		for inMsg := range inbox {
			switch inMsg.Content.(type) {
			case LockGrantedMessage:
				lockResponseCount += 1
			case LockAbortMessage:
				aborters[inMsg.Sender] = struct{}{}
			default:
				panic("Unexpected message")
			}

			if lockResponseCount == len(action.Targets)-len(aborters) {
				break WaitLocks
			}
		}

		if len(aborters) > 0 {
			for address := range action.Targets {
				if _, found := aborters[address]; found {
					continue
				}

				outbox <- OutboundMessage{Target: address, Content: LockAbortMessage{txid: txid}}
			}

			time.Sleep(time.Millisecond * 5)
			continue Try
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
