package drpdelta

import "github.com/google/uuid"

type Inspector struct {
	address Address
	Values  <-chan any
}

func (o *orchestrator) NewInspector(dep ReactiveNode) *Inspector {
	address := Address("inspector-" + uuid.New().String())
	inbox, _ := o.RegisterActor(address)
	values := make(chan any, 1024)
	initialValue, _ := dep.Subscribe(address)
	values <- initialValue
	go func() {
		for msg := range inbox {
			change := msg.Content.(ChangeMessage)
			values <- change.value
		}
	}()
	return &Inspector{
		address: address,
		Values:  values,
	}
}
