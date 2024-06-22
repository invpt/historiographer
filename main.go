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
	x := rt.Definition(func(dep higrt.Depend) any { return dep(a) })
	y := rt.Definition(func(dep higrt.Depend) any { return dep(a) })
	z := rt.Definition(func(dep higrt.Depend) any { return dep(x).(int) + dep(y).(int) })

	fmt.Println("Address of a:", a)
	fmt.Println("Address of x:", x)
	fmt.Println("Address of y:", y)
	fmt.Println("Address of z:", z)

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

	for zValue := range rt.Inspect(z) {
		fmt.Println("New value of z:", zValue)
	}

}
