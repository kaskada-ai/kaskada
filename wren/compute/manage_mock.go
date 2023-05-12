package compute

type ComputeManagerMockConfig struct {
}

// GetComputeManagerMockConfig returns a default ComputeManagerMockConfig with all methods returning nil
func GetComputeManagerMockConfig() ComputeManagerMockConfig {
	return ComputeManagerMockConfig{}
}

type computeManagerMock struct {
	config ComputeManagerMockConfig
}

// NewKaskadaTableMockClient creates a new KaskadaTableClient for testing purposes
func NewComputeManagerMock(config ComputeManagerMockConfig) ComputeManager {
	return &computeManagerMock{
		config: config,
	}
}
