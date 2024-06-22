package main

import (
	"fmt"
	"hig/higact"
	"hig/higrt"
	"time"
)

func main() {
	rt := higrt.NewRuntime(higact.NewLocalRouter())

	a := rt.Variable(0)
	fmt.Println("Address of a:", a)
	x := rt.Definition(func(dep higrt.Depend) any { return dep(a) })
	fmt.Println("Address of x:", x)
	y := rt.Definition(func(dep higrt.Depend) any { return dep(a) })
	fmt.Println("Address of y:", y)
	z := rt.Definition(func(dep higrt.Depend) any { return dep(x).(int) + dep(y).(int) })
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
