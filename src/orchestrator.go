package drpdelta

import (
	"sync"
	"time"
)

type ReactiveNode interface {
	Subscribe(Address) (value any, sources map[Address]struct{})
	Address() Address
	Run()
}

func NewOrchestrator() orchestrator {
	return orchestrator{actors: sync.Map{}}
}

type orchestrator struct {
	actors sync.Map
}

func (o *orchestrator) RegisterActor(address Address) (<-chan InboundMessage, chan<- OutboundMessage) {
	inbox := make(chan InboundMessage, 1024)
	outbox := make(chan OutboundMessage, 1024)

	o.actors.Store(address, inbox)
	go func() {
		for outbound := range outbox {
			time.Sleep(1 * time.Millisecond)
			targetInboxAny, ok := o.actors.Load(outbound.Target)
			if !ok {
				panic("Invariant broken: failed to send to inbox")
			}
			targetInbox := targetInboxAny.(chan InboundMessage)
			targetInbox <- InboundMessage{Sender: address, Content: outbound.Content}
		}
	}()

	return inbox, outbox
}
