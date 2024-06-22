package higrt

import (
	"fmt"
	"hig/higact"
	"time"
)

func Transact(r higact.Router, targets map[higact.Address]LockKind, f func(map[higact.Address]any) map[higact.Address]any) {
	actor := r.CreateActor()
	defer r.FreeActor(actor.Address)

	fmt.Println("Starting transaction,", actor.Address)

Try:
	for {
		id := newTxid(actor.Address)

		// make a list of all the things we write to for later use
		writes := []higact.Address{}
		for address, kind := range targets {
			if kind == LockKindWrite {
				writes = append(writes, address)
			}
		}

		// request all locks
		for address, kind := range targets {
			actor.Outbox <- higact.OutboundMessage{Target: address, Data: varLockAcquireMessage{Txid: id, Kind: kind}}
		}
		// wait to be granted all locks
		aborters := map[higact.Address]struct{}{}
		lockResponseCount := 0
	WaitLocks:
		for message := range actor.Inbox {
			switch message.Data.(type) {
			case varLockGrantedMessage:
				lockResponseCount += 1
			case varLockFailedMessage:
				aborters[message.Sender] = struct{}{}
			default:
				panic("Unexpected message")
			}

			if lockResponseCount == len(targets)-len(aborters) {
				break WaitLocks
			}
		}

		if lockResponseCount != len(targets)-len(aborters) {
			panic("Failed to acquire all locks for transaction")
		}

		if len(aborters) > 0 {
			for address := range targets {
				if _, found := aborters[address]; found {
					continue
				}
				actor.Outbox <- higact.OutboundMessage{Target: address, Data: varLockReleaseMessage{Txid: id}}
			}

			time.Sleep(time.Millisecond * 5)
			continue Try
		}

		// request all reads
		for name := range targets {
			actor.Outbox <- higact.OutboundMessage{Target: name, Data: varReadMessage{Tx: tx{Id: id, Writes: writes}}}
		}

		// wait to be granted all reads
		values := map[higact.Address]any{}
		requirementsSet := map[txid][]higact.Address{}
		for message := range actor.Inbox {
			if messageData, ok := message.Data.(varReadResultMessage); ok {
				values[message.Sender] = messageData.Value
				for _, tx := range messageData.ValueProvides {
					requirementsSet[tx.Id] = tx.Writes
				}

				if len(values) == len(targets) {
					break
				}
			}
		}

		requirements := make([]tx, 0, len(requirementsSet))
		for id, writes := range requirementsSet {
			requirements = append(requirements, tx{Id: id, Writes: writes})
		}

		// do all writes
		for address, value := range f(values) {
			if kind, ok := targets[address]; !ok || kind != LockKindWrite {
				panic("invariant broken: action tries to write to variable it doesn't have a write lock for")
			}

			actor.Outbox <- higact.OutboundMessage{Target: address, Data: varWriteMessage{Tx: tx{Id: id, Writes: writes}, Value: value, Requires: requirements}}
		}

		for address := range targets {
			actor.Outbox <- higact.OutboundMessage{Target: address, Data: varLockReleaseMessage{Txid: id}}
		}

		break
	}
}