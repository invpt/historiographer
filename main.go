package main

import (
	"fmt"
	"hig/higact"
	"hig/higrt"
	"time"
)

func main() {
	rt := higrt.NewRuntime(higact.NewLocalRouter())

	a := rt.NewVariable(0)
	fmt.Println("Address of a:", a.Address)
	go a.Run()
	x := rt.NewDefinition([]higact.Address{a.Address}, func(f func(higact.Address) any) any { return f(a.Address) })
	fmt.Println("Address of x:", x.Address)

	go x.Run()

	go (func() {
		rt.Transact(map[higact.Address]higrt.LockKind{a.Address: higrt.LockKindWrite}, func(m map[higact.Address]any) map[higact.Address]any { return map[higact.Address]any{a.Address: 2} })
		time.Sleep(time.Second * 2)
		rt.Transact(map[higact.Address]higrt.LockKind{a.Address: higrt.LockKindWrite}, func(m map[higact.Address]any) map[higact.Address]any { return map[higact.Address]any{a.Address: 55} })
	})()

	for xValue := range rt.Inspect(x.Address) {
		fmt.Println("New value of x:", xValue)
	}

}
