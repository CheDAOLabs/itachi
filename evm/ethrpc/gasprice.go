package ethrpc

import (
	"context"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rpc"
	//"github.com/yu-org/yu/common"
	"github.com/ethereum/go-ethereum/common"
	"math/big"
	"slices"
)

const sampleNumber = 3 // Number of transactions sampled in a block

// Oracle recommends gas prices based on the content of recent
// blocks. Suitable for both light and full clients.
type EthGasPrice struct {
	//backend     OracleBackend
	lastHead    common.Hash
	lastPrice   *big.Int
	maxPrice    *big.Int
	ignorePrice *big.Int
	//cacheLock   sync.RWMutex
	//fetchLock   sync.Mutex

	checkBlocks, percentile int
	//maxHeaderHistory, maxBlockHistory uint64

	//historyCache *lru.Cache[cacheKey, processedFees]
}

// OracleBackend includes all necessary background APIs for oracle.
//type OracleBackend interface {
//	HeaderByNumber(ctx context.Context, number rpc.BlockNumber) (*types.Header, error)
//	BlockByNumber(ctx context.Context, number rpc.BlockNumber) (*types.Block, error)
//	GetReceipts(ctx context.Context, hash common.Hash) (types.Receipts, error)
//	Pending() (*types.Block, types.Receipts, *state.StateDB)
//	ChainConfig() *params.ChainConfig
//	//SubscribeChainHeadEvent(ch chan<- core.ChainHeadEvent) event.Subscription
//}

// SuggestTipCap returns a tip cap so that newly created transaction can have a
// very high chance to be included in the following blocks.
//
// Note, for legacy transactions and the legacy eth_gasPrice RPC call, it will be
// necessary to add the basefee to the returned number to fall back to the legacy
// behavior.
func (e *EthAPIBackend) SuggestGasTipCap(ctx context.Context) (*big.Int, error) {

	head, _ := e.HeaderByNumber(ctx, rpc.LatestBlockNumber)
	headHash := head.Hash()

	var ethGasPrice *EthGasPrice

	// If the latest gasprice is still available, return it.
	//e.cacheLock.RLock()
	lastHead, lastPrice := ethGasPrice.lastHead, ethGasPrice.lastPrice
	//oracle.cacheLock.RUnlock()
	if headHash == lastHead {
		return new(big.Int).Set(lastPrice), nil
	}
	//oracle.fetchLock.Lock()
	//defer oracle.fetchLock.Unlock()

	// Try checking the cache again, maybe the last fetch fetched what we need
	//oracle.cacheLock.RLock()
	lastHead, lastPrice = ethGasPrice.lastHead, ethGasPrice.lastPrice
	//oracle.cacheLock.RUnlock()
	if headHash == lastHead {
		return new(big.Int).Set(lastPrice), nil
	}
	var (
		sent, exp int
		number    = head.Number.Uint64()
		result    = make(chan results, ethGasPrice.checkBlocks)
		quit      = make(chan struct{})
		results   []*big.Int
	)
	for sent < ethGasPrice.checkBlocks && number > 0 {
		go e.getBlockValues(ctx, number, sampleNumber, ethGasPrice.ignorePrice, result, quit)
		sent++
		exp++
		number--
	}
	for exp > 0 {
		res := <-result
		if res.err != nil {
			close(quit)
			return new(big.Int).Set(lastPrice), res.err
		}
		exp--
		// Nothing returned. There are two special cases here:
		// - The block is empty
		// - All the transactions included are sent by the miner itself.
		// In these cases, use the latest calculated price for sampling.
		if len(res.values) == 0 {
			res.values = []*big.Int{lastPrice}
		}
		// Besides, in order to collect enough data for sampling, if nothing
		// meaningful returned, try to query more blocks. But the maximum
		// is 2*checkBlocks.
		if len(res.values) == 1 && len(results)+1+exp < ethGasPrice.checkBlocks*2 && number > 0 {
			go e.getBlockValues(ctx, number, sampleNumber, ethGasPrice.ignorePrice, result, quit)
			sent++
			exp++
			number--
		}
		results = append(results, res.values...)
	}
	price := lastPrice
	if len(results) > 0 {
		slices.SortFunc(results, func(a, b *big.Int) int { return a.Cmp(b) })
		price = results[(len(results)-1)*ethGasPrice.percentile/100]
	}
	if price.Cmp(ethGasPrice.maxPrice) > 0 {
		price = new(big.Int).Set(ethGasPrice.maxPrice)
	}
	//oracle.cacheLock.Lock()
	ethGasPrice.lastHead = headHash
	ethGasPrice.lastPrice = price
	//oracle.cacheLock.Unlock()

	return new(big.Int).Set(price), nil
}

type results struct {
	values []*big.Int
	err    error
}

// getBlockValues calculates the lowest transaction gas price in a given block
// and sends it to the result channel. If the block is empty or all transactions
// are sent by the miner itself(it doesn't make any sense to include this kind of
// transaction prices for sampling), nil gasprice is returned.
func (e *EthAPIBackend) getBlockValues(ctx context.Context, blockNum uint64, limit int, ignoreUnder *big.Int, result chan results, quit chan struct{}) {
	block, err := e.BlockByNumber(ctx, rpc.BlockNumber(blockNum))
	if block == nil {
		select {
		case result <- results{nil, err}:
		case <-quit:
		}
		return
	}
	signer := types.MakeSigner(e.ChainConfig(), block.Number(), block.Time())

	// Sort the transaction by effective tip in ascending sort.
	txs := block.Transactions()
	sortedTxs := make([]*types.Transaction, len(txs))
	copy(sortedTxs, txs)
	baseFee := block.BaseFee()
	slices.SortFunc(sortedTxs, func(a, b *types.Transaction) int {
		// It's okay to discard the error because a tx would never be
		// accepted into a block with an invalid effective tip.
		tip1, _ := a.EffectiveGasTip(baseFee)
		tip2, _ := b.EffectiveGasTip(baseFee)
		return tip1.Cmp(tip2)
	})

	var prices []*big.Int
	for _, tx := range sortedTxs {
		tip, _ := tx.EffectiveGasTip(baseFee)
		if ignoreUnder != nil && tip.Cmp(ignoreUnder) == -1 {
			continue
		}
		sender, err := types.Sender(signer, tx)
		if err == nil && sender != block.Coinbase() {
			prices = append(prices, tip)
			if len(prices) >= limit {
				break
			}
		}
	}
	select {
	case result <- results{prices, nil}:
	case <-quit:
	}
}
