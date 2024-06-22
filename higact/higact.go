package higact

import (
	"fmt"
	"reflect"

	"github.com/google/uuid"
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

func (a Address) String() string {
	return a.value
}

type Inbox <-chan InboundMessage
type Outbox chan<- OutboundMessage

type Actor struct {
	Address
	Inbox
	Outbox
}

func NewLocalRouter() *LocalRouter {
	return &LocalRouter{actors: map[Address]localActor{}}
}

type LocalRouter struct {
	actors map[Address]localActor
}

type localActor struct {
	inbox  chan InboundMessage
	outbox chan OutboundMessage
}

func (r *LocalRouter) CreateActor() Actor {
	address := generateAddress()
	inbox := make(chan InboundMessage, 1024)
	outbox := make(chan OutboundMessage, 1024)

	r.actors[address] = localActor{inbox: inbox, outbox: outbox}

	go r.handleOutboxMessages(address)

	return Actor{Address: address, Inbox: inbox, Outbox: outbox}
}

func (r *LocalRouter) handleOutboxMessages(address Address) {
	for message := range r.actors[address].outbox {
		fmt.Println("----> Message from", address, "to", message.Target, "with", reflect.TypeOf(message.Data), message.Data)
		r.actors[message.Target].inbox <- InboundMessage{Sender: address, Data: message.Data}
	}
}

func (r *LocalRouter) FreeActor(address Address) {
	close(r.actors[address].inbox)
	close(r.actors[address].outbox)
	delete(r.actors, address)
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
