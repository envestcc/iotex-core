package main

import (
	_ "embed"
	"flag"
	"fmt"
	"html/template"
	"os"
	"os/exec"
	"path"
	"time"

	"go.uber.org/zap"

	"github.com/iotexproject/iotex-core/pkg/log"
)

var (
	nodeBinaryPath          string
	nodeDataPath            string
	archiveSnapshotCapacity = 1000000 // historical blocks to keep in snapshot
	archiveSnapshotReserve  = 256     // blocks to keep in before snapshot
	snapshotIndexStart      = 1       // start index of snapshot
	backupRoot              string    // backup folder

	//go:embed config-snapshot.tmpl
	configTmpl string
	//go:embed genesis-snapshot.tmpl
	genesisTmpl string
)

type SnapshotConfig struct {
	StopHeight uint64
	DataPath   string
}

func initParams() {
	flag.StringVar(&nodeBinaryPath, "node-binary", "./bin/server", "path to iotex-core binary")
	flag.StringVar(&nodeDataPath, "data", "./archive/data", "path to iotex-core data folder")
	flag.IntVar(&snapshotIndexStart, "start", 1, "start index of snapshot")
	flag.StringVar(&backupRoot, "backup", "./backup", "root folder to store backup")
	flag.Parse()
}

func main() {
	initParams()
	// TODO: read state height to determine the start index of snapshot

	for i := snapshotIndexStart; ; i++ {
		if err := genSnapshot(i); err != nil {
			log.L().Error("Failed to generate snapshot", zap.Int("index", i), zap.Error(err))
			break
		}
		copySnapshot(i)
		go backupSnapshot(i)
		time.Sleep(time.Second)
	}
}

func genSnapshot(index int) error {
	log.L().Info("Generating snapshot", zap.Int("index", index))
	// generate config and genesis files
	configPath, genesisPath, err := genSnapshotConfig(index)
	if err != nil {
		return err
	}
	// run node to generate snapshot
	cmd := exec.Command(nodeBinaryPath, "-config-path="+configPath, "-genesis-path="+genesisPath)
	log.L().Info("cmd", zap.String("cmd", cmd.String()))
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err = cmd.Run()
	// output, err := cmd.Output()
	// fmt.Println(string(output))
	fmt.Println(err)
	// TODO: check if snapshot is generated successfully
	return nil
}

func genSnapshotConfig(index int) (string, string, error) {
	configPath := fmt.Sprintf("config-snapshot-%d.yaml", index)
	genesisPath := fmt.Sprintf("genesis-snapshot-%d.yaml", index)
	// TODO: generate config and genesis files
	// Create a new template and parse the template into it
	t := template.Must(template.New("SnapshotConfig").Parse(configTmpl))
	t2 := template.Must(template.New("SnapshotConfig").Parse(genesisTmpl))
	// Create a new file
	file, err := os.Create(configPath)
	if err != nil {
		return "", "", err
	}
	defer file.Close()
	err = t.Execute(file, SnapshotConfig{StopHeight: snapshotStartHeight(index), DataPath: nodeDataPath})
	if err != nil {
		return "", "", err
	}
	// Create a new file
	file2, err := os.Create(genesisPath)
	if err != nil {
		return "", "", err
	}
	defer file2.Close()
	err = t2.Execute(file2, SnapshotConfig{})
	if err != nil {
		return "", "", err
	}
	return configPath, genesisPath, nil
}

func snapshotStartHeight(index int) uint64 {
	start := index*archiveSnapshotCapacity - archiveSnapshotReserve
	if start <= 0 {
		return 1
	}
	return uint64(start)
}

func snapshotStopHeight(index int) uint64 {
	return uint64((index + 1) * archiveSnapshotCapacity)
}

func snapshotFolder(index int) string {
	return fmt.Sprintf("%s/snapshots/shard-%03d", backupRoot, index)
}

func copySnapshot(index int) {
	log.L().Info("Copying snapshot", zap.Int("index", index))
	files := []string{"trie.db"}
	folder := snapshotFolder(index)
	// create backup folder
	cmd := exec.Command("mkdir", "-p", folder)
	output, err := cmd.Output()
	fmt.Println(string(output))
	fmt.Println(err)
	// copy files to backup folder
	for _, file := range files {
		filePath := path.Join(nodeDataPath, "data", file)
		cmd := exec.Command("cp", filePath, folder)
		output, err := cmd.Output()
		fmt.Println(string(output))
		fmt.Println(err)
	}
}

func backupSnapshot(index int) {
	// TODO: backup snapshot to remote storage
	log.L().Info("Backing up snapshot async", zap.Int("index", index))
	// exec.Command("gstuil", "cp", "-r",  , "gs://iotex-snapshot")
}
