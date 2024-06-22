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
	x := rt.Definition([]higact.Address{a}, func(f func(higact.Address) any) any { return f(a) })
	fmt.Println("Address of x:", x)
	y := rt.Definition([]higact.Address{a}, func(f func(higact.Address) any) any { return f(a) })
	fmt.Println("Address of y:", y)
	z := rt.Definition([]higact.Address{x, y}, func(f func(higact.Address) any) any { return f(x).(int) + f(y).(int) })
	fmt.Println("Address of z:", z)

	go (func() {
		rt.Transact(map[higact.Address]higrt.LockKind{a: higrt.LockKindWrite}, func(m map[higact.Address]any) map[higact.Address]any { return map[higact.Address]any{a: 2} })
		time.Sleep(time.Second * 2)
		rt.Transact(map[higact.Address]higrt.LockKind{a: higrt.LockKindWrite}, func(m map[higact.Address]any) map[higact.Address]any { return map[higact.Address]any{a: 55} })
	})()

	for zValue := range rt.Inspect(z) {
		fmt.Println("New value of z:", zValue)
	}

}
