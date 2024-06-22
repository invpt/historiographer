package higrt

import "hig/higact"

type Runtime struct {
	router higact.Router
}

func NewRuntime(router higact.Router) *Runtime {
	return &Runtime{router: router}
}
