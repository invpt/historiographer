package drpdelta

type ReactiveNode interface {
	Subscribe(Address)
	Address() Address
}
