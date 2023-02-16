package utils

type ParallelizeConfig struct {
	// The number of parallel Prepare requests that are made per api request
	PrepareFactor int
	// The number of parallel Query requests that are made per api request
	QueryFactor int
}
