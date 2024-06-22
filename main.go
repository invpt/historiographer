package main

import (
	"fmt"
	"hig/higact"
	"hig/higrt"
	"time"
)

func main() {
	rt := higrt.NewRuntime(higact.NewLocalRouter())

	// Dependency graph:
	//   a        (state variable)
	//  / \
	// x   y      (definitions)
	//  \ /
	//   z        (definition)
	a := rt.Variable(0)
	x := rt.Definition(func(dep higrt.Depend) any { return dep(a).(int) * 2 })
	y := rt.Definition(func(dep higrt.Depend) any { return dep(a).(int)*3 + 35 })
	z := rt.Definition(func(dep higrt.Depend) any { return dep(x).(int) + dep(y).(int)*4 })

	fmt.Println("Address of a:", a)
	fmt.Println("Address of x:", x)
	fmt.Println("Address of y:", y)
	fmt.Println("Address of z:", z)

	// Run some transactions
	go (func() {
		rt.Transact(
			map[higact.Address]higrt.LockKind{a: higrt.LockKindWrite},
			func(m map[higact.Address]any) map[higact.Address]any {
				return map[higact.Address]any{a: 2}
			},
		)
		time.Sleep(time.Second * 2)
		rt.Transact(
			map[higact.Address]higrt.LockKind{a: higrt.LockKindWrite},
			func(m map[higact.Address]any) map[higact.Address]any {
				return map[higact.Address]any{a: 55}
			},
		)
	})()

	// Inspect some values
	go (func() {
		for value := range rt.Inspect(a) {
			fmt.Println("New value of a:", value)
		}
	})()
	go (func() {
		for value := range rt.Inspect(x) {
			fmt.Println("New value of x:", value)
		}
	})()
	go (func() {
		for value := range rt.Inspect(y) {
			fmt.Println("New value of y:", value)
		}
	})()
	go (func() {
		for value := range rt.Inspect(z) {
			fmt.Println("New value of z:", value)
		}
	})()

	// Stop the program from stopping immediately
	var unused string
	fmt.Scanln(&unused)
}
