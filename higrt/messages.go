package higrt

import (
	"hig/higact"
	"time"
)

// A transaction ID plus the set of names written to by the transaction.
type tx struct {
	// the transaction's ID
	Id txid
	// the list of values written to by the transaction
	Writes []higact.Address
}

type txid struct {
	secs    int64
	nanos   int
	creator higact.Address
}

func newTxid(creator higact.Address) txid {
	now := time.Now()
	return txid{
		secs:    now.Unix(),
		nanos:   now.Nanosecond(),
		creator: creator,
	}
}

func (t txid) Lt(other txid) bool {
	return t.secs < other.secs || t.secs == other.secs && t.nanos < other.nanos || t.secs == other.secs && t.nanos == other.nanos && t.creator.String() < other.creator.String()
}

/////////////////////////////////////////////
/////////////////////////////////////////////
//////// reactive node message types ////////
/////////////////////////////////////////////
/////////////////////////////////////////////

type subscribeMessage struct{}

type subscriptionGrantedMessage struct {
	Value             any
	Provides          []tx
	AncestorVariables []higact.Address
}

type changeMessage struct {
	Value    any
	Provides []tx
	Requires []tx
}

////////////////////////////////////////////
////////////////////////////////////////////
//////// var-specific message types ////////
////////////////////////////////////////////
////////////////////////////////////////////

type varLockAcquireMessage struct {
	Txid txid
	Kind LockKind
}

type varLockReleaseMessage struct {
	Txid txid
}

type varReadMessage struct {
	Tx tx
}

type varWriteMessage struct {
	Tx tx

	// The new value
	Value any
	// The required transactions for the new value to be valid
	Requires []tx
}

type varLockFailedMessage struct {
	Txid txid
}

type varLockGrantedMessage struct {
	Txid txid
}

type varReadResultMessage struct {
	Txid          txid
	Value         any
	ValueProvides []tx
}

////////////////////////////////////////////
////////////////////////////////////////////
//////// def-specific message types ////////
////////////////////////////////////////////
////////////////////////////////////////////

type defReadMessage struct {
	Txid     txid
	Requires []tx
}

type defReadResultMessage struct {
	Txid  txid
	Value any
}
