package higrt

import (
	"hig/higact"
)

func (rt *Runtime) Inspect(what higact.Address) <-chan any {
	actor := rt.router.CreateActor()
	values := make(chan any, 1024)

	go (func() {
		actor.Outbox <- higact.OutboundMessage{Target: what, Data: subscribeMessage{}}
		subscription := (<-actor.Inbox).Data.(subscriptionGrantedMessage)

		values <- subscription.Value

		for message := range actor.Inbox {
			switch messageData := message.Data.(type) {
			case changeMessage:
				values <- messageData.Value
			}
		}

		rt.router.FreeActor(actor.Address)
	})()

	return values
}
