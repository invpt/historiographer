package main

import (
	drpdelta "drp-delta/src"
	"fmt"
)

func main() {
	o := drpdelta.NewOrchestrator()

	x := o.NewVariable("x", 0)
	y := o.NewVariable("y", 0)
	z := o.NewDefinition("z", []drpdelta.ReactiveNode{x, y}, func(m map[drpdelta.Address]any) any {
		return m["x"].(int) + m["y"].(int)
	})
	go z.Run()
	go y.Run()
	go x.Run()
	zVal := o.NewInspector(z)

	go o.DoAction(drpdelta.Action{
		Targets: map[drpdelta.Address]drpdelta.LockKind{
			(x.Address()): drpdelta.LockKindWrite,
		},
		F: func(m map[drpdelta.Address]any) map[drpdelta.Address]any {
			return map[drpdelta.Address]any{
				(x.Address()): m[x.Address()].(int) + 1,
			}
		},
	})

	go o.DoAction(drpdelta.Action{
		Targets: map[drpdelta.Address]drpdelta.LockKind{
			(x.Address()): drpdelta.LockKindWrite,
		},
		F: func(m map[drpdelta.Address]any) map[drpdelta.Address]any {
			return map[drpdelta.Address]any{
				(x.Address()): m[x.Address()].(int) + 1,
			}
		},
	})

	go o.DoAction(drpdelta.Action{
		Targets: map[drpdelta.Address]drpdelta.LockKind{
			(x.Address()): drpdelta.LockKindWrite,
			(y.Address()): drpdelta.LockKindWrite,
		},
		F: func(m map[drpdelta.Address]any) map[drpdelta.Address]any {
			return map[drpdelta.Address]any{
				(x.Address()): m[x.Address()].(int) + 1,
				(y.Address()): m[y.Address()].(int) + 1,
			}
		},
	})

	go o.DoAction(drpdelta.Action{
		Targets: map[drpdelta.Address]drpdelta.LockKind{
			(x.Address()): drpdelta.LockKindWrite,
			(y.Address()): drpdelta.LockKindWrite,
		},
		F: func(m map[drpdelta.Address]any) map[drpdelta.Address]any {
			return map[drpdelta.Address]any{
				(x.Address()): m[x.Address()].(int) + 1,
				(y.Address()): m[y.Address()].(int) + 1,
			}
		},
	})

	for val := range zVal.Values {
		fmt.Println("New value from z:", val)
	}
}
