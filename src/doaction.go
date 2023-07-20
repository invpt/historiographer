package drpdelta

import "fmt"

func DoAction(action Action, inbox <-chan InboundMessage, outbox chan<- OutboundMessage) {
	txid := NewTxid()

	// make a list of all the things we write to for later use
	writes := []Address{}
	for name, kind := range action.targets {
		if kind == write {
			writes = append(writes, name)
		}
	}

	// request all queues
	for name := range action.targets {
		outbox <- OutboundMessage{Target: name, Content: QueueMessage{txid: txid}}
	}
	// wait to be granted all queues
	queueResponseCount := 0
	for inMsg := range inbox {
		if _, ok := inMsg.Content.(QueueGrantedMessage); ok {
			queueResponseCount += 1

			if queueResponseCount == len(action.targets) {
				break
			}
		} else {
			fmt.Println("Unexpected message:", inMsg)
			panic("")
		}
	}

	// request all locks
	for name, kind := range action.targets {
		outbox <- OutboundMessage{Target: name, Content: LockMessage{txid: txid, kind: kind}}
	}
	// wait to be granted all locks
	lockResponseCount := 0
	for inMsg := range inbox {
		if _, ok := inMsg.Content.(LockGrantedMessage); ok {
			lockResponseCount += 1

			if lockResponseCount == len(action.targets) {
				break
			}
		} else {
			fmt.Println("Unexpected message:", inMsg)
			panic("")
		}
	}

	// request all reads
	for name := range action.targets {
		outbox <- OutboundMessage{Target: name, Content: ReadMessage{tx: Tx{id: txid, writes: writes}}}
	}
	// wait to be granted all reads
	values := map[Address]any{}
	preds := []Tx{}
	for inMsg := range inbox {
		if inContent, ok := inMsg.Content.(ReadResultMessage); ok {
			values[inMsg.Sender] = inContent.value
			preds = append(preds, inContent.valueTx)
		}
	}

	// do all writes
	for name, value := range action.f(values) {
		if kind, ok := action.targets[name]; !ok || kind != write {
			panic("invariant broken: action tries to write to variable it doesn't have a write lock for")
		}

		outbox <- OutboundMessage{Target: name, Content: WriteMessage{tx: Tx{id: txid, writes: writes}, preds: preds, value: value}}
	}
}

type Action struct {
	targets map[Address]lockKind
	f       func(map[Address]any) map[Address]any
}
