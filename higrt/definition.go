package higrt

import (
	"hig/higact"
)

type Definition struct {
	higact.Actor

	// Monotonic counter used to order received changes.
	counter int

	changes map[changeLink]*change
	applied map[txid]tx

	replicas map[higact.Address]any

	f             func(func(higact.Address) any) any
	value         any
	valueProvides []tx

	pendingReads []pendingRead

	descendentInputs map[higact.Address][]higact.Address
	subscribers      []higact.Address
}

type pendingRead struct {
	sender   higact.Address
	txid     txid
	requires []tx
}

type change struct {
	// The position of this change in the total order of changes received by this definition
	order int
	// The address of the input this change is for
	input higact.Address
	// The input's new value
	value any
	// The transactions provided by this change
	provides []tx
	// The transactions required by this change
	requires []tx
	// The links from this change to other changes in the change dependency graph
	links map[changeLink]struct{}
}

type changeLink struct {
	txid    txid
	address higact.Address
}

func NewDefinition(r higact.Router, deps []higact.Address, f func(func(higact.Address) any) any) *Definition {
	actor := r.CreateActor()

	d := &Definition{
		Actor:            actor,
		f:                f,
		replicas:         make(map[higact.Address]any, len(deps)),
		descendentInputs: map[higact.Address][]higact.Address{},
		changes:          map[changeLink]*change{},
		applied:          map[txid]tx{},
	}

	d.replicas = make(map[higact.Address]any, len(deps))
	d.descendentInputs = map[higact.Address][]higact.Address{}
	for _, dep := range deps {
		d.Outbox <- higact.OutboundMessage{Target: dep, Data: subscribeMessage{}}
		subscription := (<-d.Inbox).Data.(subscriptionGrantedMessage)
		for _, variable := range subscription.AncestorVariables {
			d.descendentInputs[variable] = append(d.descendentInputs[variable], dep)
		}
		d.replicas[dep] = subscription.Value
		d.value = f(func(a higact.Address) any { return d.replicas[a] })
	}

	return d
}

func (d *Definition) Run() {
	for message := range d.Inbox {
		switch messageData := message.Data.(type) {
		case changeMessage:
			changeVal := d.changeFromMessage(message.Sender, messageData)
			c := &changeVal

			for _, tx := range c.provides {
				d.changes[changeLink{tx.Id, c.input}] = c
			}
		case subscribeMessage:
			who := message.Sender

			d.subscribers = append(d.subscribers, who)

			sourcesMap := map[higact.Address]struct{}{}
			for _, vars := range d.descendentInputs {
				for _, varName := range vars {
					sourcesMap[varName] = struct{}{}
				}
			}
			sources := make([]higact.Address, 0, len(sourcesMap))
			for source := range sourcesMap {
				sources = append(sources, source)
			}

			provides := make([]tx, 0, len(d.applied))
			for _, tx := range d.applied {
				provides = append(provides, tx)
			}

			d.Outbox <- higact.OutboundMessage{Target: who, Data: subscriptionGrantedMessage{Value: d.value, Provides: provides, AncestorVariables: sources}}
		case defReadMessage:
			d.pendingReads = append(d.pendingReads, pendingRead{message.Sender, messageData.Txid, messageData.Requires})
		default:
			panic("Unexpected message")
		}

		for _, batch := range d.findBatches() {
			provides, requires := d.applyBatch(batch)

			for _, tx := range provides {
				d.applied[tx.Id] = tx
			}

			for _, sub := range d.subscribers {
				d.Outbox <- higact.OutboundMessage{Target: sub, Data: changeMessage{Provides: provides, Requires: requires, Value: d.value}}
			}
		}

		for _, pending := range d.pendingReads {
			ready := true
			for _, tx := range pending.requires {
				needsTx := false
				for _, var_ := range tx.Writes {
					if len(d.descendentInputs[var_]) > 0 {
						needsTx = true
						break
					}
				}

				if _, found := d.applied[tx.Id]; needsTx && !found {
					ready = false
					break
				}
			}

			if ready {
				d.Outbox <- higact.OutboundMessage{Target: pending.sender, Data: defReadResultMessage{pending.txid, d.value}}
			}
		}
	}
}

func (d *Definition) lookupReplicaValue(address higact.Address) any {
	return d.replicas[address]
}

// Converts a `ChangeMessage` to the internal `change` type.
func (d *Definition) changeFromMessage(sender higact.Address, message changeMessage) change {
	links := map[changeLink]struct{}{}
	for _, tx := range message.Provides {
		links[changeLink{tx.Id, sender}] = struct{}{}
	}
	for _, tx := range message.Requires {
		for _, var_ := range tx.Writes {
			for _, name := range d.descendentInputs[var_] {
				links[changeLink{tx.Id, name}] = struct{}{}
			}
		}
	}

	change := change{
		order:    d.counter,
		input:    sender,
		value:    message.Value,
		provides: message.Provides,
		requires: message.Requires,
		links:    links,
	}

	d.counter += 1

	return change
}

///////////////////////////////////////////////
///////////////////////////////////////////////
//////// batch-finding related methods ////////
///////////////////////////////////////////////
///////////////////////////////////////////////

type tarjan struct {
	stack []*change

	index   map[*change]int
	lowlink map[*change]int
	tainted map[*change]bool
	onStack map[*change]bool

	components       []tarjanComponent
	indexCounter     int
	componentTainted bool
}

type tarjanComponent struct {
	changes map[*change]struct{}
	tainted bool
}

// Finds all batches that can be executed now. Returns in order of allowable execution.
//
// Implemented using Tarjan's algorithm for strongly connected components.
func (d *Definition) findBatches() (batches []map[*change]struct{}) {
	stateVal := tarjan{indexCounter: 1, index: map[*change]int{}, lowlink: map[*change]int{}, tainted: map[*change]bool{}, onStack: map[*change]bool{}}
	state := &stateVal

	for _, v := range d.changes {
		if state.index[v] == 0 {
			d.scc(state, v)
		}
	}

	for _, component := range state.components {
		if !component.tainted {
			batches = append(batches, component.changes)
		}
	}

	return
}

// Visits the given vertex `v` under the given Tarjan `state`.
func (d *Definition) scc(state *tarjan, v *change) {
	state.index[v] = state.indexCounter
	state.lowlink[v] = state.indexCounter
	state.indexCounter += 1
	state.stack = append(state.stack, v)
	state.onStack[v] = true

	for link := range v.links {
		if _, applied := d.applied[link.txid]; applied {
			continue
		}

		w := d.changes[link]

		if w == nil || state.tainted[w] {
			// if w doesn't exist or if w is from a tainted
			// component, mark the current component as tainted
			// (state.tainted[w] implies !state.onStack[w])
			state.componentTainted = true
		} else if state.index[w] == 0 {
			d.scc(state, w)
			if state.lowlink[w] < state.lowlink[v] {
				state.lowlink[v] = state.lowlink[w]
			}
		} else if state.onStack[w] && state.index[w] < state.lowlink[v] {
			state.lowlink[v] = state.index[w]
		}
	}

	if state.lowlink[v] == state.index[v] {
		changes := map[*change]struct{}{}
		for {
			w := state.stack[len(state.stack)-1]
			state.stack = state.stack[:len(state.stack)-1]
			delete(state.onStack, w)
			if state.componentTainted {
				state.tainted[w] = true
			}
			changes[w] = struct{}{}

			if w == v {
				break
			}
		}
		state.components = append(state.components, tarjanComponent{changes, state.componentTainted})
		state.componentTainted = false
	}
}

///////////////////////////////////////////////////
///////////////////////////////////////////////////
//////// batch application related methods ////////
///////////////////////////////////////////////////
///////////////////////////////////////////////////

// Applies a batch.
func (d *Definition) applyBatch(batch map[*change]struct{}) (provides []tx, requires []tx) {
	provides = d.providedTransactions(batch)
	requires = d.requiredTransactions(batch)

	maxApplied := make(map[higact.Address]int, len(d.replicas))
	for change := range batch {
		for _, tx := range change.provides {
			delete(d.changes, changeLink{tx.Id, change.input})
		}

		// we're iterating the batch in an arbitrary order. to maintain consistency, all we need to
		// do is make sure that we don't apply changes for an input if later changes were already applied
		if change.order < maxApplied[change.input] {
			continue
		}

		d.replicas[change.input] = change.value
		maxApplied[change.input] = change.order
	}

	d.value = d.f(d.lookupReplicaValue)
	d.valueProvides = provides

	return
}

// Computes the list of transactions provided by an entire batch.
func (d *Definition) providedTransactions(batch map[*change]struct{}) (provided []tx) {
	providedMap := map[txid][]higact.Address{}

	for change := range batch {
		for _, tx := range change.provides {
			providedMap[tx.Id] = tx.Writes
		}
	}

	provided = make([]tx, len(providedMap))
	for id, writes := range providedMap {
		provided = append(provided, tx{Id: id, Writes: writes})
	}

	return
}

// Computes the list of transactions required by an entire batch.
func (d *Definition) requiredTransactions(batch map[*change]struct{}) (required []tx) {
	requiredMap := map[txid][]higact.Address{}

	for change := range batch {
		for _, tx := range change.requires {
			requiredMap[tx.Id] = tx.Writes
		}
	}

	for _, tx := range d.valueProvides {
		requiredMap[tx.Id] = tx.Writes
	}

	required = make([]tx, len(requiredMap))
	for id, writes := range requiredMap {
		required = append(required, tx{Id: id, Writes: writes})
	}

	return
}