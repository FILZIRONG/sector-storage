package storiface

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/filecoin-project/sector-storage/sealtasks"
)

type WorkerInfo struct {
	Hostname string

	TaskResourcesLk sync.Mutex                         // 添加的内容
	TaskResources   map[sealtasks.TaskType]*TaskConfig // 添加的内容
	Resources       WorkerResources
}

type WorkerResources struct {
	MemPhysical uint64
	MemSwap     uint64

	MemReserved uint64 // Used by system / other processes

	CPUs uint64 // Logical cores
	GPUs []string
}

type WorkerStats struct {
	Info WorkerInfo

	MemUsedMin uint64
	MemUsedMax uint64
	GpuUsed    bool
	CpuUse     uint64
}

//===================添加的内容====================//
type TaskConfig struct {
	LimitCount int
	RunCount   int
}

func NewTaskLimitConfig(jsonname string) map[sealtasks.TaskType]*TaskConfig {
	type taskLimitConfig struct {
		AddPiece   int
		PreCommit1 int
		PreCommit2 int
		Commit1    int
		Commit2    int
		Fetch      int
		Finalize   int
	}
	config := &taskLimitConfig{
		AddPiece:   1,
		PreCommit1: 1,
		PreCommit2: 1,
		Commit1:    1,
		Commit2:    1,
		Fetch:      1,
		Finalize:   1,
	}

	configFileName := ""
	if env, ok := os.LookupEnv("LOTUS_STORAGE_PATH"); ok {
		jsonFile := filepath.Join(env, jsonname)
		if _, err := os.Stat(jsonFile); err == nil { // 文件存在
			configFileName = jsonFile
		}
	}
	if configFileName == "" {
		if env, ok := os.LookupEnv("WORKER_PATH"); ok {
			jsonFile := filepath.Join(env, jsonname)
			if _, err := os.Stat(jsonFile); err == nil { // 文件存在
				configFileName = jsonFile
			} else {
				configFileName = "~/.lotusstorage/" + jsonname
			}
		}
	}

	fmt.Println("Config file: ", configFileName)
	configFileName, _ = filepath.Abs(configFileName)
	configFile, err := os.Open(configFileName)
	if err != nil {
		fmt.Println("read config json file error!")
	}
	defer configFile.Close()
	jsonParser := json.NewDecoder(configFile)
	if err := jsonParser.Decode(config); err != nil {
		fmt.Println("parse config json file error!")
	}

	cfgResources := make(map[sealtasks.TaskType]*TaskConfig)

	if _, ok := cfgResources[sealtasks.TTAddPiece]; !ok {
		cfgResources[sealtasks.TTAddPiece] = &TaskConfig{
			LimitCount: config.AddPiece,
			RunCount:   0,
		}
	}

	if _, ok := cfgResources[sealtasks.TTPreCommit1]; !ok {
		cfgResources[sealtasks.TTPreCommit1] = &TaskConfig{
			LimitCount: config.PreCommit1,
			RunCount:   0,
		}
	}

	if _, ok := cfgResources[sealtasks.TTPreCommit2]; !ok {
		cfgResources[sealtasks.TTPreCommit2] = &TaskConfig{
			LimitCount: config.PreCommit2,
			RunCount:   0,
		}
	}

	if _, ok := cfgResources[sealtasks.TTCommit1]; !ok {
		cfgResources[sealtasks.TTCommit1] = &TaskConfig{
			LimitCount: config.Commit1,
			RunCount:   0,
		}
	}

	if _, ok := cfgResources[sealtasks.TTCommit2]; !ok {
		cfgResources[sealtasks.TTCommit2] = &TaskConfig{
			LimitCount: config.Commit2,
			RunCount:   0,
		}
	}

	if _, ok := cfgResources[sealtasks.TTFetch]; !ok {
		cfgResources[sealtasks.TTFetch] = &TaskConfig{
			LimitCount: config.Fetch,
			RunCount:   0,
		}
	}

	if _, ok := cfgResources[sealtasks.TTFinalize]; !ok {
		cfgResources[sealtasks.TTFinalize] = &TaskConfig{
			LimitCount: config.Finalize,
			RunCount:   0,
		}
	}

	return cfgResources
}

//===================添加的内容====================//
