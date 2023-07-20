package drpdelta

import (
	"time"
)

type InboundMessage struct {
	Sender  Address
	Content any
}

type OutboundMessage struct {
	Target  Address
	Content any
}

type Address string

type LockMessage struct {
	txid Txid
	kind LockKind
}

type ReadMessage struct {
	tx Tx
}

type WriteMessage struct {
	tx    Tx
	preds []Tx
	value any
}

type LockAbortMessage struct {
	txid Txid
}

type LockGrantedMessage struct {
	txid Txid
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

type Txid struct {
	secs    int64
	nanos   int
	address Address
}

func NewTxid(address Address) Txid {
	now := time.Now()
	return Txid{
		secs:    now.Unix(),
		nanos:   now.Nanosecond(),
		address: address,
	}
}

func (t Txid) Lt(other Txid) bool {
	return t.secs < other.secs || t.secs == other.secs && t.nanos < other.nanos || t.secs == other.secs && t.nanos == other.nanos && t.address < other.address
}
