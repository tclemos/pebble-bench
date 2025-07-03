package benchmark

import (
	"math/rand"
)

// Predefined transaction type profiles
var TransactionTypeProfiles = map[string]TransactionCharacteristics{
	"simple_transfer": {
		GasUsed:              21000,
		AccountsTouched:      2,
		StorageOpsPerAccount: 0,
		CallDepth:            0,
		EventsEmitted:        0,
		TransactionType:      "simple_transfer",
	},
	"erc20_transfer": {
		GasUsed:              65000,
		AccountsTouched:      3,
		StorageOpsPerAccount: 1.0,
		CallDepth:            1,
		EventsEmitted:        1,
		TransactionType:      "erc20_transfer",
	},
	"uniswap_swap": {
		GasUsed:              150000,
		AccountsTouched:      4,
		StorageOpsPerAccount: 2.5,
		CallDepth:            2,
		EventsEmitted:        3,
		TransactionType:      "uniswap_swap",
	},
	"complex_defi": {
		GasUsed:              300000,
		AccountsTouched:      8,
		StorageOpsPerAccount: 4.0,
		CallDepth:            4,
		EventsEmitted:        6,
		TransactionType:      "complex_defi",
	},
	"contract_deployment": {
		GasUsed:              800000,
		AccountsTouched:      2,
		StorageOpsPerAccount: 3.0,
		CallDepth:            1,
		EventsEmitted:        1,
		TransactionType:      "contract_deployment",
	},
}

// TransactionMixConfig defines the distribution of transaction types
type TransactionMixConfig struct {
	SimpleTransferRatio   float64 `json:"simple_transfer_ratio"`
	ERC20TransferRatio    float64 `json:"erc20_transfer_ratio"`
	UniswapSwapRatio      float64 `json:"uniswap_swap_ratio"`
	ComplexDeFiRatio      float64 `json:"complex_defi_ratio"`
	ContractDeployRatio   float64 `json:"contract_deploy_ratio"`
}

// Predefined transaction mix profiles
var (
	BalancedTransactionMix = TransactionMixConfig{
		SimpleTransferRatio: 0.25,
		ERC20TransferRatio:  0.25,
		UniswapSwapRatio:    0.25,
		ComplexDeFiRatio:    0.15,
		ContractDeployRatio: 0.10,
	}

	EthereumMainnetMix = TransactionMixConfig{
		SimpleTransferRatio: 0.30,
		ERC20TransferRatio:  0.25,
		UniswapSwapRatio:    0.20,
		ComplexDeFiRatio:    0.15,
		ContractDeployRatio: 0.10,
	}

	PolygonPoSMix = TransactionMixConfig{
		SimpleTransferRatio: 0.20,
		ERC20TransferRatio:  0.30,
		UniswapSwapRatio:    0.25,
		ComplexDeFiRatio:    0.20,
		ContractDeployRatio: 0.05,
	}

	DeFiHeavyMix = TransactionMixConfig{
		SimpleTransferRatio: 0.15,
		ERC20TransferRatio:  0.20,
		UniswapSwapRatio:    0.35,
		ComplexDeFiRatio:    0.25,
		ContractDeployRatio: 0.05,
	}

	TransferHeavyMix = TransactionMixConfig{
		SimpleTransferRatio: 0.50,
		ERC20TransferRatio:  0.35,
		UniswapSwapRatio:    0.10,
		ComplexDeFiRatio:    0.03,
		ContractDeployRatio: 0.02,
	}
)

// TransactionGenerator creates realistic transaction mixes
type TransactionGenerator struct {
	model       *TransactionModel
	mixConfig   TransactionMixConfig
	rng         *rand.Rand
	
	// Pre-computed cumulative weights for faster selection
	cumulativeWeights []float64
	typeNames         []string
}

// NewTransactionGenerator creates a new transaction generator
func NewTransactionGenerator(model *TransactionModel, mixConfig TransactionMixConfig, seed int64) *TransactionGenerator {
	generator := &TransactionGenerator{
		model:     model,
		mixConfig: mixConfig,
		rng:       rand.New(rand.NewSource(seed)),
	}
	
	// Pre-compute cumulative weights for efficient selection
	generator.computeCumulativeWeights()
	
	return generator
}

// computeCumulativeWeights calculates cumulative distribution for transaction type selection
func (tg *TransactionGenerator) computeCumulativeWeights() {
	weights := []float64{
		tg.mixConfig.SimpleTransferRatio,
		tg.mixConfig.ERC20TransferRatio,
		tg.mixConfig.UniswapSwapRatio,
		tg.mixConfig.ComplexDeFiRatio,
		tg.mixConfig.ContractDeployRatio,
	}
	
	tg.typeNames = []string{
		"simple_transfer",
		"erc20_transfer",
		"uniswap_swap",
		"complex_defi",
		"contract_deployment",
	}
	
	// Normalize weights to sum to 1.0
	total := 0.0
	for _, w := range weights {
		total += w
	}
	
	if total == 0 {
		// Fallback to equal distribution
		for i := range weights {
			weights[i] = 1.0 / float64(len(weights))
		}
		total = 1.0
	}
	
	// Calculate cumulative weights
	tg.cumulativeWeights = make([]float64, len(weights))
	cumulative := 0.0
	for i, w := range weights {
		cumulative += w / total
		tg.cumulativeWeights[i] = cumulative
	}
}

// GenerateTransaction creates a transaction with characteristics based on type distribution
func (tg *TransactionGenerator) GenerateTransaction() TransactionCharacteristics {
	txType := tg.selectTransactionType()
	baseChars := TransactionTypeProfiles[txType]
	
	// Add some realistic variance
	return tg.addVariance(baseChars)
}

// selectTransactionType chooses transaction type based on weights
func (tg *TransactionGenerator) selectTransactionType() string {
	r := tg.rng.Float64()
	
	for i, cumWeight := range tg.cumulativeWeights {
		if r <= cumWeight {
			return tg.typeNames[i]
		}
	}
	
	// Fallback to simple transfer
	return "simple_transfer"
}

// addVariance introduces realistic parameter variation
func (tg *TransactionGenerator) addVariance(base TransactionCharacteristics) TransactionCharacteristics {
	varied := base
	
	// Add ±20% variance to gas usage
	gasVariance := 1.0 + (tg.rng.Float64()-0.5)*0.4
	varied.GasUsed = uint64(float64(base.GasUsed) * gasVariance)
	
	// Add variance to accounts touched (±1, with minimum of 1)
	if tg.rng.Float64() < 0.3 {
		varied.AccountsTouched += tg.rng.Intn(3) - 1
		if varied.AccountsTouched < 1 {
			varied.AccountsTouched = 1
		}
	}
	
	// Add variance to storage operations (±50%)
	if base.StorageOpsPerAccount > 0 {
		storageVariance := 1.0 + (tg.rng.Float64()-0.5)
		varied.StorageOpsPerAccount = base.StorageOpsPerAccount * storageVariance
		if varied.StorageOpsPerAccount < 0 {
			varied.StorageOpsPerAccount = 0
		}
	}
	
	// Add variance to call depth (occasionally deeper calls)
	if base.CallDepth > 0 && tg.rng.Float64() < 0.2 {
		varied.CallDepth += tg.rng.Intn(2) // 0 or 1 additional depth
		if varied.CallDepth > 10 {         // Reasonable maximum
			varied.CallDepth = 10
		}
	}
	
	// Add variance to events emitted
	if base.EventsEmitted > 0 && tg.rng.Float64() < 0.3 {
		varied.EventsEmitted += tg.rng.Intn(3) - 1 // -1, 0, or +1
		if varied.EventsEmitted < 0 {
			varied.EventsEmitted = 0
		}
	}
	
	return varied
}

// GetTransactionMixFromString returns predefined transaction mix configuration
func GetTransactionMixFromString(mixType string) TransactionMixConfig {
	switch mixType {
	case "ethereum":
		return EthereumMainnetMix
	case "polygon":
		return PolygonPoSMix
	case "defi-heavy":
		return DeFiHeavyMix
	case "transfer-heavy":
		return TransferHeavyMix
	case "balanced":
		fallthrough
	default:
		return BalancedTransactionMix
	}
}

// GetModelConfigFromString returns predefined model configuration
func GetModelConfigFromString(networkType string) TransactionModelConfig {
	switch networkType {
	case "ethereum":
		return EthereumMainnetConfig
	case "polygon":
		return PolygonPosConfig
	case "testnet":
		return TestnetConfig
	default:
		return EthereumMainnetConfig // Default to Ethereum
	}
}

// ValidateTransactionMix ensures transaction mix ratios are valid
func ValidateTransactionMix(mix TransactionMixConfig) bool {
	total := mix.SimpleTransferRatio + mix.ERC20TransferRatio + 
			 mix.UniswapSwapRatio + mix.ComplexDeFiRatio + mix.ContractDeployRatio
	
	// Allow some tolerance for floating point precision
	return total >= 0.99 && total <= 1.01 && 
		   mix.SimpleTransferRatio >= 0 && mix.ERC20TransferRatio >= 0 &&
		   mix.UniswapSwapRatio >= 0 && mix.ComplexDeFiRatio >= 0 &&
		   mix.ContractDeployRatio >= 0
}