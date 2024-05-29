package api

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"

	"github.com/ethereum/go-ethereum/rpc"
	"github.com/pkg/errors"
	"github.com/tidwall/gjson"
	"go.uber.org/zap"

	"github.com/iotexproject/go-pkgs/hash"
	"github.com/iotexproject/go-pkgs/util"

	"github.com/iotexproject/iotex-core/blockchain/blockdao"
	"github.com/iotexproject/iotex-core/pkg/log"
)

type (
	proxyHandler struct {
		proxys map[uint64]*httputil.ReverseProxy
		shards []ProxyShard
		dao    blockdao.BlockDAO
	}
)

func newProxyHandler(shards []ProxyShard, dao blockdao.BlockDAO) (*proxyHandler, error) {
	h := &proxyHandler{
		proxys: make(map[uint64]*httputil.ReverseProxy),
		shards: shards,
		dao:    dao,
	}
	if len(shards) == 0 {
		return nil, errors.New("no shards provided")
	}
	for _, shard := range shards {
		targetURL, err := url.Parse(shard.Endpoint)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to parse endpoint %s", shard.Endpoint)
		}
		proxy := httputil.NewSingleHostReverseProxy(targetURL)
		h.proxys[shard.ID] = proxy
	}
	return h, nil
}

func (handler *proxyHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != "POST" {
		handler.proxys[0].ServeHTTP(w, req)
		return
	}
	bodyBytes, err := io.ReadAll(req.Body)
	if err != nil {
		handler.error(w, err)
		return
	}
	web3Reqs, err := parseWeb3Reqs(io.NopCloser(bytes.NewBuffer(bodyBytes)))
	if err != nil {
		handler.error(w, err)
		return
	}
	req.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
	if !web3Reqs.IsArray() {
		blkNum, err := handler.parseWeb3Height(&web3Reqs)
		if err != nil {
			log.L().Error("failed to parse block number", zap.Error(err))
			blkNum = rpc.LatestBlockNumber
		}
		shard := handler.shardByHeight(blkNum)
		appendShardToRequest(req, shard)
		log.L().Info("forwarding request to shard", zap.Uint64("shard", shard.ID), zap.String("request height", blkNum.String()), zap.String("endpoint", shard.Endpoint))
		handler.proxys[shard.ID].ServeHTTP(w, req)
	} else {
		// TODO: batch request
		shard := handler.shardByHeight(rpc.LatestBlockNumber)
		appendShardToRequest(req, shard)
		handler.proxys[shard.ID].ServeHTTP(w, req)
	}
}

func (handler *proxyHandler) error(w http.ResponseWriter, err error) {
	log.L().Error("failed to get request body", zap.Error(err))
	raw, err := json.Marshal(&web3Response{err: err})
	if err != nil {
		log.L().Error("failed to marshal error response", zap.Error(err))
		w.WriteHeader(http.StatusInternalServerError)
	} else {
		w.Write(raw)
	}
}

func (handler *proxyHandler) shardByHeight(height rpc.BlockNumber) *ProxyShard {
	shards := handler.shards
	// if height is latest, return the last shard
	if height.Int64() < 0 {
		return &shards[len(shards)-1]
	}
	// find the shard by historical height
	for _, shard := range shards {
		if uint64(height.Int64()) >= shard.StartHeight && uint64(height.Int64()) < shard.EndHeight {
			return &shard
		}
	}
	// if not found, return the last shard
	return &shards[len(shards)-1]
}

func (handler *proxyHandler) parseWeb3Height(web3Req *gjson.Result) (rpc.BlockNumber, error) {
	var (
		method = web3Req.Get("method").Value()
		blkNum rpc.BlockNumber
		err    error
	)
	switch method {
	case "eth_getBalance", "eth_getCode", "eth_getTransactionCount", "eth_call", "eth_estimateGas", "debug_traceCall":
		blkParam := web3Req.Get("params.1")
		blkNum, err = parseBlockNumber(&blkParam)
	case "eth_getStorageAt":
		blkParam := web3Req.Get("params.2")
		blkNum, err = parseBlockNumber(&blkParam)
	case "debug_traceBlockByNumber":
		blkParam := web3Req.Get("params.0")
		blkNum, err = parseBlockNumber(&blkParam)
	case "debug_traceTransaction", "debug_traceBlockByHash":
		blkHash := web3Req.Get("params.0")
		hash, err := hash.HexStringToHash256(util.Remove0xPrefix(blkHash.String()))
		if err != nil {
			return 0, err
		}
		height, err := handler.dao.GetBlockHeight(hash)
		if err != nil {
			return 0, err
		}
		blkNum = rpc.BlockNumber(height)
	default:
		blkNum = rpc.LatestBlockNumber
	}
	return blkNum, err
}

func appendShardToRequest(r *http.Request, shard *ProxyShard) {
	r.URL.Query().Add("shard", strconv.FormatUint(shard.ID, 10))
}
