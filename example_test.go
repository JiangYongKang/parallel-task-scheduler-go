package scheduler_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	scheduler "github.com/highcumontoa/parallel-task-scheduler-go"
)

// 展示：依赖调度、命名资源、有限重试与退避、结果与统计。
func Example() {
	s := scheduler.New(scheduler.Config{
		MaxConcurrency: 2,
		NamedResources: map[string]int{"db": 1},
	})

	attempts := 0
	specs := []scheduler.TaskSpec{
		{ID: "extract", Run: func(ctx context.Context) error { return nil }},
		{
			ID: "transform", Deps: []string{"extract"},
			Resources:   scheduler.Resources{Named: map[string]int{"db": 1}},
			MaxAttempts: 3,
			Backoff:     time.Millisecond,
			Run: func(ctx context.Context) error {
				attempts++
				if attempts < 2 {
					return errors.New("transient db hiccup")
				}
				return nil
			},
		},
		{ID: "load", Deps: []string{"transform"}, Run: func(ctx context.Context) error { return nil }},
	}

	if err := s.Submit(specs); err != nil {
		fmt.Println("submit error:", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	results, stats, err := s.Run(ctx)
	if err != nil {
		fmt.Println("run error:", err)
		return
	}
	for _, id := range []string{"extract", "transform", "load"} {
		fmt.Printf("%s=%s\n", id, results[id].State)
	}
	fmt.Printf("succeeded=%d totalAttempts=%d\n", stats.Succeeded, stats.TotalAttempts)
	// Output:
	// extract=succeeded
	// transform=succeeded
	// load=succeeded
	// succeeded=3 totalAttempts=4
}

// 展示可区分的校验错误：成环。
func ExampleBuildDAG_cycle() {
	ok := func(context.Context) error { return nil }
	_, err := scheduler.BuildDAG([]scheduler.TaskSpec{
		{ID: "a", Deps: []string{"b"}, Run: ok},
		{ID: "b", Deps: []string{"a"}, Run: ok},
	}, scheduler.Resources{Slots: 4})
	var ve *scheduler.ValidationError
	if errors.As(err, &ve) {
		fmt.Println(ve.Kind)
	}
	// Output: cyclic_dependency
}
