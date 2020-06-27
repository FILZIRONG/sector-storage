package sectorstorage

import (
	"context"

	"golang.org/x/xerrors"

	"github.com/filecoin-project/specs-actors/actors/abi"

	"github.com/filecoin-project/sector-storage/sealtasks"
	"github.com/filecoin-project/sector-storage/stores"
)

type taskSelector struct {
	best []stores.StorageInfo //nolint: unused, structcheck
}

func newTaskSelector() *taskSelector {
	return &taskSelector{}
}

func (s *taskSelector) Ok(ctx context.Context, task sealtasks.TaskType, spt abi.RegisteredSealProof, whnd *workerHandle) (bool, error) {
	tasks, err := whnd.w.TaskTypes(ctx)
	if err != nil {
		return false, xerrors.Errorf("getting supported worker task types: %w", err)
	}
	_, supported := tasks[task]

	return supported, nil
}

func (s *taskSelector) Cmp(ctx context.Context, _ sealtasks.TaskType, a, b *workerHandle) (bool, error) {
	atasks, err := a.w.TaskTypes(ctx)
	if err != nil {
		return false, xerrors.Errorf("getting supported worker task types: %w", err)
	}
	btasks, err := b.w.TaskTypes(ctx)
	if err != nil {
		return false, xerrors.Errorf("getting supported worker task types: %w", err)
	}
	if len(atasks) != len(btasks) {
		return len(atasks) < len(btasks), nil // prefer workers which can do less
	}

	return a.active.utilization(a.info.Resources) < b.active.utilization(b.info.Resources), nil
}

var _ WorkerSelector = &allocSelector{}

//===================添加的内容====================//
func (s *taskSelector) FindDataWoker(ctx context.Context, task sealtasks.TaskType, sid abi.SectorID, whnd *workerHandle) bool {
	tasks, err := whnd.w.TaskTypes(ctx)
	if err != nil {
		return false
	}
	if _, supported := tasks[task]; !supported {
		return false
	}

	paths, err := whnd.w.Paths(ctx)
	if err != nil {
		return false
	}

	have := map[stores.ID]struct{}{}
	for _, path := range paths {
		have[path.ID] = struct{}{}
	}

	for _, info := range s.best {
		if info.Weight != 0 { // 为0的权重是fecth来的，不是本地的
			if _, ok := have[info.ID]; ok {
				return true
			}
		}
	}

	return false
}

//===================添加的内容====================//
