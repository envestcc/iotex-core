package main

import (
	"errors"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"

	"github.com/tidwall/gjson"
	"go.uber.org/zap"

	"github.com/iotexproject/go-pkgs/util"

	"github.com/iotexproject/iotex-core/pkg/log"
)

type Shard struct {
	id          uint64
	startHeight uint64
	endHeight   uint64
}

var (
	shards = []Shard{
		{0, 0, 1000000},
		{1, 100000, 2000000},
		{2, 2000000, 3000000},
	}
)

func main() {
	targetURL, err := url.Parse("http://127.0.0.1:15014")
	if err != nil {
		log.L().Fatal("failed to parse target url", zap.Error(err))
	}
	proxy := httputil.NewSingleHostReverseProxy(targetURL)

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		body, err := r.GetBody()
		if err != nil {
			log.L().Error("failed to get request body", zap.Error(err))
			// TODO: return error as web3 response
			http.Error(w, "failed to get request body", http.StatusInternalServerError)
			return
		}
		web3Reqs, err := parseWeb3Reqs(body)
		if !web3Reqs.IsArray() {
			height, err := parseWeb3Height(&web3Reqs)
			if err != nil {
				log.L().Error("failed to parse block number", zap.Error(err))
				height = 0
			}
			shard := shardByHeight(height)
			appendShardToRequest(r, shard)
			proxy.ServeHTTP(w, r)
		} else {
			// TODO: batch request
			shard := shardByHeight(0)
			appendShardToRequest(r, shard)
			proxy.ServeHTTP(w, r)
		}
	})

	log.L().Fatal("proxy server stopped", zap.Error(http.ListenAndServe(":5014", nil)))
}

func parseWeb3Reqs(reader io.Reader) (gjson.Result, error) {
	data, err := io.ReadAll(reader)
	if err != nil {
		return gjson.Result{}, err
	}
	if !gjson.Valid(string(data)) {
		return gjson.Result{}, errors.New("request json format is not valid")
	}
	ret := gjson.Parse(string(data))
	// check rquired field
	for _, req := range ret.Array() {
		id := req.Get("id")
		method := req.Get("method")
		if !id.Exists() || !method.Exists() {
			return gjson.Result{}, errors.New("request field is incomplete")
		}
	}
	return ret, nil
}

func parseWeb3Height(web3Req *gjson.Result) (uint64, error) {
	var (
		method = web3Req.Get("method").Value()
	)
	switch method {
	case "eth_getBalance", "eth_getCode", "eth_getTransactionCount", "eth_call", "eth_estimateGas":
		blkNum := web3Req.Get("params.1")
		return parseBlockNumber(blkNum.String())
	case "eth_getStorageAt":
		blkNum := web3Req.Get("params.2")
		return parseBlockNumber(blkNum.String())
	}
	return 0, nil
}

func shardByHeight(height uint64) *Shard {
	// if height is 0, return the last shard
	if height == 0 {
		return &shards[len(shards)-1]
	}
	// find the shard by historical height
	for _, shard := range shards {
		if height >= shard.startHeight && height < shard.endHeight {
			return &shard
		}
	}
	// if not found, return the last shard
	return &shards[len(shards)-1]
}

func appendShardToRequest(r *http.Request, shard *Shard) {
	r.URL.Query().Add("shard", strconv.FormatUint(shard.id, 10))
}

func parseBlockNumber(str string) (uint64, error) {
	switch str {
	case "earliest":
		return 1, nil
	case "", "latest", "pending":
		return 0, nil
	default:
		return strconv.ParseUint(util.Remove0xPrefix(str), 16, 64)
	}
}
