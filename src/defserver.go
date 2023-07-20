package drpdelta

func (d *definition) server(inbox <-chan InboundMessage, outbox chan<- OutboundMessage) {
	for inMsg := range inbox {
		switch inContent := inMsg.Content.(type) {
		case ChangeMessage:
			inContentRef := &inContent

			d.changeLists[inMsg.Sender] = append(d.changeLists[inMsg.Sender], inContentRef)
			for _, tx := range inContent.txs {
				txid := tx.id
				if _, found := d.changeGraph[txid]; !found {
					d.changeGraph[txid] = map[Address]*ChangeMessage{}
				}

				d.changeGraph[txid][inMsg.Sender] = inContentRef
			}

			for _, pred := range inContent.preds {
				if _, found := d.changeGraph[pred.id]; !found {
					d.changeGraph[pred.id] = map[Address]*ChangeMessage{}
					for _, name := range pred.writes {
						d.changeGraph[pred.id][name] = nil
					}
				}
			}
		}

		d.tick(inbox, outbox)
	}
}

func (d *definition) tick(inbox <-chan InboundMessage, outbox chan<- OutboundMessage) {
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

		for inputName, changeList := range d.changeLists {
			for i, change := range changeList {
				if _, found := smallestTransitivePreds[change]; found {
					// execute the change
					d.replicas[inputName] = change.value
					delete(smallestTransitivePreds, change)
				} else {
					d.changeLists[inputName] = d.changeLists[inputName][i:]
					break
				}
			}
		}

		if len(smallestTransitivePreds) > 0 {
			panic("invariant broken: didn't exhaust the transitive preds set!")
		}

		newValue := d.f(d.replicas)

		for _, sub := range d.subscribers {
			outbox <- OutboundMessage{Target: sub, Content: ChangeMessage{txs: allTxs, preds: allPreds, value: newValue}}
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

	for change, _ := range changes {
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

func NewDefinition(address Address, deps []ReactiveNode, f func(map[Address]any) any) definition {
	replicas := map[Address]any{}
	changeLists := map[Address][]*ChangeMessage{}
	for _, dep := range deps {
		dep.Subscribe(address)
		replicas[dep.Address()] = nil
		changeLists[dep.Address()] = []*ChangeMessage{}
	}

	return definition{
		Address:                     address,
		replicas:                    replicas,
		changeGraph:                 map[Txid]map[Address]*ChangeMessage{},
		changeLists:                 changeLists,
		f:                           f,
		subscribers:                 []Address{},
		inputTransitiveVariableDeps: map[Address]map[Address]struct{}{}, // TODO: properly impl this
	}
}

func (d *definition) Subscribe(who Address) {
	d.subscribers = append(d.subscribers, who)
}

type definition struct {
	Address                     Address
	replicas                    map[Address]any
	changeGraph                 map[Txid]map[Address]*ChangeMessage
	changeLists                 map[Address][]*ChangeMessage
	f                           func(map[Address]any) any
	subscribers                 []Address
	inputTransitiveVariableDeps map[Address]map[Address]struct{}
}
