package main

import (
	"time"

	"github.com/iotexproject/iotex-core/pkg/log"
)

func main() {
	log.L().Info("Running fake node")
	for i := 0; ; i++ {
		time.Sleep(time.Second)
		log.L().Info("Generating snapshot")
		if i >= 10 {
			log.L().Panic("Failed to generate snapshot")
		}
	}
}
