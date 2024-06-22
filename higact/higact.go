package higact

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/puzpuzpuz/xsync/v3"
)

type Router interface {
	CreateActor() Actor
	FreeActor(Address)
}

type OutboundMessage struct {
	Target Address
	Data   any
}

type InboundMessage struct {
	Sender Address
	Data   any
}

type Address struct {
	value string
}

func (a Address) Lt(b Address) bool {
	return a.value < b.value
}

type Inbox <-chan InboundMessage
type Outbox chan<- OutboundMessage

type Actor struct {
	Address
	Inbox
	Outbox
}

func NewLocalRouter() *LocalRouter {
	return &LocalRouter{actors: xsync.NewMapOf[Address, localActor]()}
}

type LocalRouter struct {
	actors *xsync.MapOf[Address, localActor]
}

type localActor struct {
	inbox  chan InboundMessage
	outbox chan OutboundMessage
}

func (r *LocalRouter) CreateActor() Actor {
	address := generateAddress()
	inbox := make(chan InboundMessage, 1024)
	outbox := make(chan OutboundMessage, 1024)

	r.actors.Store(address, localActor{inbox: inbox, outbox: outbox})

	go r.handleOutboxMessages(address)

	return Actor{Address: address, Inbox: inbox, Outbox: outbox}
}

func (r *LocalRouter) handleOutboxMessages(address Address) {
	actor, ok := r.actors.Load(address)
	if !ok {
		panic("handleOutboxMessages called with bad address")
	}

	for message := range actor.outbox {
		fmt.Printf("----> Message from %v to %v with %#v\n", address, message.Target, message.Data)
		target, ok := r.actors.Load(message.Target)
		if !ok {
			panic(fmt.Sprintf("Message sent to nonexistent actor with address %v", message.Target))
		}
		target.inbox <- InboundMessage{Sender: address, Data: message.Data}
	}
}

func (r *LocalRouter) FreeActor(address Address) {
	actor, ok := r.actors.Load(address)
	if !ok {
		panic("Attempted to free nonexistent actor (double free?)")
	}
	close(actor.inbox)
	close(actor.outbox)
	r.actors.Delete(address)
}

var counter int = 0

func generateAddress() Address {
	if false {
		return Address{value: uuid.New().String()}
	} else {
		counter += 1
		return Address{value: fmt.Sprint("actor", counter)}
	}
}
