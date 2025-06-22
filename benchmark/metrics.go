package benchmark

// Metrics holds benchmark performance results
type Metrics struct {
	TotalReads     int
	TotalWrites    int
	ReadLatencyMs  []float64
	WriteLatencyMs []float64
	// TODO: Add cache stats and export methods
}
