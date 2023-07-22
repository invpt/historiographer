package drpdelta

func (d *definition) Run() {
	for inMsg := range d.inbox {
		switch inContent := inMsg.Content.(type) {
		case ChangeMessage:
			inContentRef := &inContent

			d.changeLists[inMsg.Sender] = append(d.changeLists[inMsg.Sender], inContentRef)
			for _, tx := range inContentRef.txs {
				if _, found := d.changeGraph[tx.id]; !found {
					d.changeGraph[tx.id] = map[Address]*ChangeMessage{}
				}

				d.changeGraph[tx.id][inMsg.Sender] = inContentRef
			}

			for _, pred := range inContentRef.preds {
				if _, found := d.changeGraph[pred.id]; !found {
					d.changeGraph[pred.id] = map[Address]*ChangeMessage{}
				}

				for _, name := range pred.writes {
					if _, found := d.changeGraph[pred.id][name]; !found {
						d.changeGraph[pred.id][name] = nil
					}
				}
			}
		default:
			panic("Unexpected message")
		}

		d.tick()
	}
}

func (d *definition) tick() {
	for {
		var smallestTransitivePreds map[*ChangeMessage]struct{}
		for _, changeList := range d.changeLists {
			if len(changeList) == 0 {
				continue
			}

			change := changeList[0]

			if transitivePreds, ok := d.findTransitivePreds(change); ok {
				if smallestTransitivePreds == nil || len(transitivePreds) < len(smallestTransitivePreds) {
					smallestTransitivePreds = transitivePreds
				}
			}
		}

		if smallestTransitivePreds != nil {
			allTxs, allPreds := unionTxsPreds(smallestTransitivePreds)
			allPreds = append(allPreds, d.prevTxs...)

			for inputName, changeList := range d.changeLists {
				var finalI int
				for i, change := range changeList {
					if _, found := smallestTransitivePreds[change]; found {
						// execute the change
						d.replicas[inputName] = change.value
						d.appliedChanges[change] = struct{}{}
						delete(smallestTransitivePreds, change)
						finalI = i + 1
					} else {
						break
					}
				}
				d.changeLists[inputName] = d.changeLists[inputName][finalI:]
			}

			if len(smallestTransitivePreds) > 0 {
				panic("invariant broken: didn't exhaust the transitive preds set!")
			}

			newValue := d.f(d.replicas)

			for _, sub := range d.subscribers {
				d.outbox <- OutboundMessage{Target: sub, Content: ChangeMessage{txs: allTxs, preds: allPreds, value: newValue}}
			}

			d.prevTxs = allTxs
		} else {
			break
		}
	}
}

func (d *definition) findTransitivePreds(change *ChangeMessage) (map[*ChangeMessage]struct{}, bool) {
	preds := map[*ChangeMessage]struct{}{}
	ok := d._findTransitivePreds(change, preds)
	return preds, ok
}

func (d *definition) _findTransitivePreds(change *ChangeMessage, preds map[*ChangeMessage]struct{}) bool {
	if _, found := preds[change]; found {
		return true
	}
	if _, found := d.appliedChanges[change]; found {
		return true
	}
	preds[change] = struct{}{}

	for _, tx := range change.txs {
		var overlappingInputs []Address
		for _, name := range tx.writes {
			for inputName, inputTransitiveVariableDeps := range d.inputTransitiveVariableDeps {
				if _, found := inputTransitiveVariableDeps[name]; found {
					overlappingInputs = append(overlappingInputs, inputName)
				}
			}
		}

		for _, overlappingInput := range overlappingInputs {
			thisInputChange := d.changeGraph[tx.id][overlappingInput]

			if thisInputChange == nil {
				// this input has not yet received this pred
				return false
			} else if !d._findTransitivePreds(thisInputChange, preds) {
				// the recursive findTransitivePreds call failed
				return false
			}
		}
	}

	for _, changePred := range change.preds {
		if namesToChanges, found := d.changeGraph[changePred.id]; found {
			if namesToChanges == nil {
				panic("invariant 1 broken in _findTransitivePreds")
			}

			var overlappingInputs []Address
			for name := range namesToChanges {
				for inputName, inputTransitiveVariableDeps := range d.inputTransitiveVariableDeps {
					if _, found := inputTransitiveVariableDeps[name]; found {
						overlappingInputs = append(overlappingInputs, inputName)
					}
				}
			}

			for _, overlappingInput := range overlappingInputs {
				thisInputChange := d.changeGraph[changePred.id][overlappingInput]

				if thisInputChange == nil {
					// this input has not yet received this pred
					return false
				} else if !d._findTransitivePreds(thisInputChange, preds) {
					// the recursive findTransitivePreds call failed
					return false
				}
			}
		} else {
			panic("invariant 2 broken in _findTransitivePreds")
		}
	}

	return true
}

func unionTxsPreds(changes map[*ChangeMessage]struct{}) (txs []Tx, preds []Tx) {
	txsMap := map[Txid][]Address{}
	predsMap := map[Txid][]Address{}

	for change := range changes {
		for _, tx := range change.txs {
			txsMap[tx.id] = tx.writes
		}
		for _, tx := range change.preds {
			predsMap[tx.id] = tx.writes
		}
	}

	txs = make([]Tx, len(txsMap))
	preds = make([]Tx, len(predsMap))
	for id, writes := range txsMap {
		txs = append(txs, Tx{id, writes})
	}
	for id, writes := range predsMap {
		preds = append(txs, Tx{id, writes})
	}

	return
}

func (o *orchestrator) NewDefinition(address Address, deps []ReactiveNode, f func(map[Address]any) any) *definition {
	inbox, outbox := o.RegisterActor(address)

	replicas := map[Address]any{}
	changeLists := map[Address][]*ChangeMessage{}
	inputTransitiveVariableDeps := map[Address]map[Address]struct{}{}
	for _, dep := range deps {
		replicas[dep.Address()], inputTransitiveVariableDeps[dep.Address()] = dep.Subscribe(address)
		changeLists[dep.Address()] = []*ChangeMessage{}
	}

	currentValue := f(replicas)

	return &definition{
		address:                     address,
		inbox:                       inbox,
		outbox:                      outbox,
		replicas:                    replicas,
		changeGraph:                 map[Txid]map[Address]*ChangeMessage{},
		changeLists:                 changeLists,
		appliedChanges:              map[*ChangeMessage]struct{}{},
		f:                           f,
		currentValue:                currentValue,
		subscribers:                 []Address{},
		inputTransitiveVariableDeps: inputTransitiveVariableDeps,
	}
}

func (d *definition) Subscribe(who Address) (any, map[Address]struct{}) {
	d.subscribers = append(d.subscribers, who)

	sources := map[Address]struct{}{}

	for _, inputSources := range d.inputTransitiveVariableDeps {
		for sourceAddress := range inputSources {
			sources[sourceAddress] = struct{}{}
		}
	}

	return d.currentValue, sources
}

func (d *definition) Address() Address {
	return d.address
}

type definition struct {
	address                     Address
	inbox                       <-chan InboundMessage
	outbox                      chan<- OutboundMessage
	replicas                    map[Address]any
	changeGraph                 map[Txid]map[Address]*ChangeMessage
	changeLists                 map[Address][]*ChangeMessage
	appliedChanges              map[*ChangeMessage]struct{}
	prevTxs                     []Tx
	f                           func(map[Address]any) any
	currentValue                any
	subscribers                 []Address
	inputTransitiveVariableDeps map[Address]map[Address]struct{}
}
