package drpdelta

import "github.com/google/uuid"

type InboundMessage struct {
	Sender  Address
	Content any
}

type OutboundMessage struct {
	Target  Address
	Content any
}

type Address string

type QueueMessage struct {
	txid Txid
}

type LockMessage struct {
	txid Txid
	kind lockKind
}

type ReadMessage struct {
	tx Tx
}

type WriteMessage struct {
	tx    Tx
	preds []Tx
	value any
}

type QueueGrantedMessage struct {
	txid Txid
}

type LockGrantedMessage struct {
	txid Txid
	kind lockKind
}

type ReadResultMessage struct {
	txid    Txid
	value   any
	valueTx Tx
}

type ChangeMessage struct {
	txs   []Tx
	preds []Tx
	value any
}

// A transaction ID plus the set of names written to by the transaction.
type Tx struct {
	// the transaction's ID
	id Txid
	// the list of values written to by the transaction
	writes []Address
}

type Txid string

func NewTxid() Txid {
	return Txid(uuid.New().String())
}
