package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
	"go.uber.org/zap"

	"github.com/iotexproject/go-pkgs/util"

	"github.com/iotexproject/iotex-core/pkg/log"
)

var (
	configPath string
)

func init() {
	flag.StringVar(&configPath, "config-path", "", "Config path")
	flag.Parse()
}

func main() {
	cfg, err := newConfig(configPath)
	if err != nil {
		log.L().Fatal("failed to load config", zap.Error(err))
	}
	log.S().Infof("Config in use: %+v\n", cfg)
	shardProxys := make(map[uint64]*httputil.ReverseProxy)
	for _, shard := range cfg.Shards {
		targetURL, err := url.Parse(shard.Endpoint)
		if err != nil {
			log.L().Fatal("failed to parse target url", zap.Error(err))
		}
		proxy := httputil.NewSingleHostReverseProxy(targetURL)
		shardProxys[shard.ID] = proxy
	}

	r := gin.Default()
	r.POST("/", func(c *gin.Context) {
		body, err := c.GetRawData()
		if err != nil {
			log.L().Error("failed to get request body", zap.Error(err))
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		}
		web3Reqs, err := parseWeb3Reqs(body)
		if err != nil {
			log.L().Error("failed to parse request", zap.Error(err))
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		}
		c.Request.Body = io.NopCloser(bytes.NewReader(body))
		if !web3Reqs.IsArray() {
			height, err := parseWeb3Height(&web3Reqs)
			if err != nil {
				log.L().Error("failed to parse block number", zap.Error(err))
				height = 0
			}
			shard := shardByHeight(cfg.Shards, height)
			appendShardToRequest(c.Request, shard)
			log.L().Info("forwarding request to shard", zap.Uint64("shard", shard.ID), zap.Uint64("height", height), zap.String("endpoint", shard.Endpoint))
			shardProxys[shard.ID].ServeHTTP(c.Writer, c.Request)
		} else {
			// TODO: batch request
			shard := shardByHeight(cfg.Shards, 0)
			appendShardToRequest(c.Request, shard)
			shardProxys[shard.ID].ServeHTTP(c.Writer, c.Request)
		}
	})
	err = r.Run(fmt.Sprintf(":%d", cfg.Port))
	if err != nil {
		log.L().Fatal("failed to start server", zap.Error(err))
	}
}

func parseWeb3Reqs(data []byte) (gjson.Result, error) {
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
	case "eth_getBalance", "eth_getCode", "eth_getTransactionCount", "eth_call", "eth_estimateGas", "debug_traceCall":
		blkNum := web3Req.Get("params.1")
		return parseBlockNumber(blkNum.String())
	case "eth_getStorageAt":
		blkNum := web3Req.Get("params.2")
		return parseBlockNumber(blkNum.String())
	case "debug_traceBlockByNumber":
		blkNum := web3Req.Get("params.0")
		return parseBlockNumber(blkNum.String())
	case "debug_traceTransaction", "debug_traceBlockByHash":
		// TODO: get height from transaction/block hash
	}
	return 0, nil
}

func shardByHeight(shards []Shard, height uint64) *Shard {
	// if height is 0, return the last shard
	if height == 0 {
		return &shards[len(shards)-1]
	}
	// find the shard by historical height
	for _, shard := range shards {
		if height >= shard.StartHeight && height < shard.EndHeight {
			return &shard
		}
	}
	// if not found, return the last shard
	return &shards[len(shards)-1]
}

func appendShardToRequest(r *http.Request, shard *Shard) {
	r.URL.Query().Add("shard", strconv.FormatUint(shard.ID, 10))
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
