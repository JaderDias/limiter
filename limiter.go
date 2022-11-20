package limiter

func BoundedConcurrencyWithDoneProcessor[C any](
	limit,
	jobs int,
	job func(int) C,
	done func(C),
) {
	inputChan := make(chan int, limit)
	defer close(inputChan)
	outputChan := make(chan C, jobs)
	defer close(outputChan)

	for i := 0; i < jobs; i++ {
		inputChan <- i
		go func(j int) {
			defer func() { <-inputChan }()
			outputChan <- job(j)
		}(i)
	}

	for i := 0; i < jobs; i++ {
		done(<-outputChan)
	}
}

func BoundedConcurrency(
	limit,
	jobs int,
	job func(int),
) {
	BoundedConcurrencyWithDoneProcessor(
		limit,
		jobs,
		func(i int) int {
			job(i)
			return i
		},
		func(int) {},
	)
}
