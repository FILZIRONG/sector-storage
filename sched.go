package sectorstorage

import (
	"container/heap"
	"context"
	"sort"
	"sync"
	"time"

	"github.com/hashicorp/go-multierror"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/specs-actors/actors/abi"

	"github.com/filecoin-project/sector-storage/sealtasks"
	"github.com/filecoin-project/sector-storage/storiface"
)

type schedPrioCtxKey int

var SchedPriorityKey schedPrioCtxKey
var DefaultSchedPriority = 0

func getPriority(ctx context.Context) int {
	sp := ctx.Value(SchedPriorityKey)
	if p, ok := sp.(int); ok {
		return p
	}

	return DefaultSchedPriority
}

func WithPriority(ctx context.Context, priority int) context.Context {
	return context.WithValue(ctx, SchedPriorityKey, priority)
}

const mib = 1 << 20

type WorkerAction func(ctx context.Context, w Worker) error

type WorkerSelector interface {
	Ok(ctx context.Context, task sealtasks.TaskType, spt abi.RegisteredSealProof, a *workerHandle) (bool, error) // true if worker is acceptable for performing a task

	Cmp(ctx context.Context, task sealtasks.TaskType, a, b *workerHandle) (bool, error)                 // true if a is preferred over b
	FindDataWoker(ctx context.Context, task sealtasks.TaskType, sid abi.SectorID, a *workerHandle) bool // 添加的内容
}

type scheduler struct {
	spt abi.RegisteredSealProof

	workersLk  sync.Mutex
	nextWorker WorkerID
	workers    map[WorkerID]*workerHandle

	newWorkers chan *workerHandle

	watchClosing  chan WorkerID
	workerClosing chan WorkerID

	schedule   chan *workerRequest
	workerFree chan WorkerID
	closing    chan struct{}

	schedQueue *requestQueue
}

func newScheduler(spt abi.RegisteredSealProof) *scheduler {
	return &scheduler{
		spt: spt,

		nextWorker: 0,
		workers:    map[WorkerID]*workerHandle{},

		newWorkers: make(chan *workerHandle),

		watchClosing:  make(chan WorkerID),
		workerClosing: make(chan WorkerID),

		schedule:   make(chan *workerRequest),
		workerFree: make(chan WorkerID),
		closing:    make(chan struct{}),

		schedQueue: &requestQueue{},
	}
}

func (sh *scheduler) Schedule(ctx context.Context, sector abi.SectorID, taskType sealtasks.TaskType, sel WorkerSelector, prepare WorkerAction, work WorkerAction) error {
	ret := make(chan workerResponse)

	select {
	case sh.schedule <- &workerRequest{
		sector:   sector,
		taskType: taskType,
		priority: getPriority(ctx),
		sel:      sel,

		prepare: prepare,
		work:    work,

		ret: ret,
		ctx: ctx,
	}:
	case <-sh.closing:
		return xerrors.New("closing")
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case resp := <-ret:
		return resp.err
	case <-sh.closing:
		return xerrors.New("closing")
	case <-ctx.Done():
		return ctx.Err()
	}
}

type workerRequest struct {
	sector   abi.SectorID
	taskType sealtasks.TaskType
	priority int // larger values more important
	sel      WorkerSelector

	prepare WorkerAction
	work    WorkerAction

	index int // The index of the item in the heap.

	ret chan<- workerResponse
	ctx context.Context
}

type workerResponse struct {
	err error
}

func (r *workerRequest) respond(err error) {
	select {
	case r.ret <- workerResponse{err: err}:
	case <-r.ctx.Done():
		log.Warnf("request got cancelled before we could respond")
	}
}

type activeResources struct {
	memUsedMin uint64
	memUsedMax uint64
	gpuUsed    bool
	cpuUse     uint64

	cond *sync.Cond
}

type workerHandle struct {
	w Worker

	info storiface.WorkerInfo

	preparing *activeResources
	active    *activeResources
}

func (sh *scheduler) runSched() {
	go sh.runWorkerWatcher()

	for {
		select {
		case w := <-sh.newWorkers:
			sh.schedNewWorker(w)
		case wid := <-sh.workerClosing:
			sh.schedDropWorker(wid)
		case req := <-sh.schedule:
			scheduled, err := sh.maybeSchedRequest(req)
			if err != nil {
				req.respond(err)
				continue
			}
			if scheduled {
				continue
			}

			heap.Push(sh.schedQueue, req)
		case wid := <-sh.workerFree:
			sh.onWorkerFreed(wid)
		case <-sh.closing:
			sh.schedClose()
			return
		}
	}
}

func (sh *scheduler) onWorkerFreed(wid WorkerID) {
	sh.workersLk.Lock()
	w, ok := sh.workers[wid]
	sh.workersLk.Unlock()
	if !ok {
		log.Warnf("onWorkerFreed on invalid worker %d", wid)
		return
	}

	for i := 0; i < sh.schedQueue.Len(); i++ {
		req := (*sh.schedQueue)[i]

		ok, err := req.sel.Ok(req.ctx, req.taskType, sh.spt, w)
		if err != nil {
			log.Errorf("onWorkerFreed req.sel.Ok error: %+v", err)
			continue
		}

		if !ok {
			continue
		}

		scheduled, err := sh.maybeSchedRequest(req)
		if err != nil {
			req.respond(err)
			continue
		}

		if scheduled {
			heap.Remove(sh.schedQueue, i)
			i--
			continue
		}
	}
}

var selectorTimeout = 5 * time.Second

func (sh *scheduler) maybeSchedRequest0(req *workerRequest) (bool, error) {
	sh.workersLk.Lock()
	defer sh.workersLk.Unlock()

	tried := 0
	var acceptable []WorkerID

	needRes := ResourceTable[req.taskType][sh.spt]

	for wid, worker := range sh.workers {
		rpcCtx, cancel := context.WithTimeout(req.ctx, selectorTimeout)
		ok, err := req.sel.Ok(rpcCtx, req.taskType, sh.spt, worker)
		cancel()

		if err != nil {
			return false, err
		}

		if !ok {
			continue
		}
		tried++

		if !canHandleRequest(needRes, sh.spt, wid, worker.info.Resources, worker.preparing) {
			continue
		}

		acceptable = append(acceptable, wid)
	}

	if len(acceptable) > 0 {
		{
			var serr error

			sort.SliceStable(acceptable, func(i, j int) bool {
				rpcCtx, cancel := context.WithTimeout(req.ctx, selectorTimeout)
				defer cancel()
				r, err := req.sel.Cmp(rpcCtx, req.taskType, sh.workers[acceptable[i]], sh.workers[acceptable[j]])

				if err != nil {
					serr = multierror.Append(serr, err)
				}
				return r
			})

			if serr != nil {
				return false, xerrors.Errorf("error(s) selecting best worker: %w", serr)
			}
		}

		return true, sh.assignWorker(acceptable[0], sh.workers[acceptable[0]], req)
	}

	if tried == 0 {
		return false, xerrors.New("maybeSchedRequest didn't find any good workers")
	}

	return false, nil // put in waiting queue
}

func (sh *scheduler) assignWorker(wid WorkerID, w *workerHandle, req *workerRequest) error {
	needRes := ResourceTable[req.taskType][sh.spt]

	w.preparing.add(w.info.Resources, needRes)

	go func() {
		sh.taskAddOne(wid, req.taskType)          //添加的内容
		defer sh.taskReduceOne(wid, req.taskType) //添加的内容
		err := req.prepare(req.ctx, w.w)
		sh.workersLk.Lock()

		if err != nil {
			w.preparing.free(w.info.Resources, needRes)
			sh.workersLk.Unlock()

			select {
			case sh.workerFree <- wid:
			case <-sh.closing:
				log.Warnf("scheduler closed while sending response (prepare error: %+v)", err)
			}

			select {
			case req.ret <- workerResponse{err: err}:
			case <-req.ctx.Done():
				log.Warnf("request got cancelled before we could respond (prepare error: %+v)", err)
			case <-sh.closing:
				log.Warnf("scheduler closed while sending response (prepare error: %+v)", err)
			}
			return
		}

		err = w.active.withResources(sh.spt, wid, w.info.Resources, needRes, &sh.workersLk, func() error {
			w.preparing.free(w.info.Resources, needRes)
			sh.workersLk.Unlock()
			defer sh.workersLk.Lock() // we MUST return locked from this function

			select {
			case sh.workerFree <- wid:
			case <-sh.closing:
			}

			err = req.work(req.ctx, w.w)

			select {
			case req.ret <- workerResponse{err: err}:
			case <-req.ctx.Done():
				log.Warnf("request got cancelled before we could respond")
			case <-sh.closing:
				log.Warnf("scheduler closed while sending response")
			}

			return nil
		})

		sh.workersLk.Unlock()

		// This error should always be nil, since nothing is setting it, but just to be safe:
		if err != nil {
			log.Errorf("error executing worker (withResources): %+v", err)
		}
	}()

	return nil
}

func (a *activeResources) withResources(spt abi.RegisteredSealProof, id WorkerID, wr storiface.WorkerResources, r Resources, locker sync.Locker, cb func() error) error {
	a.add(wr, r)
	err := cb()
	a.free(wr, r)
	return err
}

func (a *activeResources) add(wr storiface.WorkerResources, r Resources) {
	a.gpuUsed = r.CanGPU
	if r.MultiThread() {
		a.cpuUse += wr.CPUs
	} else {
		a.cpuUse += uint64(r.Threads)
	}

	a.memUsedMin += r.MinMemory
	a.memUsedMax += r.MaxMemory
}

func (a *activeResources) free(wr storiface.WorkerResources, r Resources) {
	if r.CanGPU {
		a.gpuUsed = false
	}
	if r.MultiThread() {
		a.cpuUse -= wr.CPUs
	} else {
		a.cpuUse -= uint64(r.Threads)
	}

	a.memUsedMin -= r.MinMemory
	a.memUsedMax -= r.MaxMemory
}

func canHandleRequest(needRes Resources, spt abi.RegisteredSealProof, wid WorkerID, res storiface.WorkerResources, active *activeResources) bool {

	// TODO: dedupe needRes.BaseMinMemory per task type (don't add if that task is already running)
	minNeedMem := res.MemReserved + active.memUsedMin + needRes.MinMemory + needRes.BaseMinMemory
	if minNeedMem > res.MemPhysical {
		log.Debugf("sched: not scheduling on worker %d; not enough physical memory - need: %dM, have %dM", wid, minNeedMem/mib, res.MemPhysical/mib)
		return false
	}

	maxNeedMem := res.MemReserved + active.memUsedMax + needRes.MaxMemory + needRes.BaseMinMemory
	if spt == abi.RegisteredSealProof_StackedDrg32GiBV1 {
		maxNeedMem += MaxCachingOverhead
	}
	if spt == abi.RegisteredSealProof_StackedDrg64GiBV1 {
		maxNeedMem += MaxCachingOverhead * 2 // ewwrhmwh
	}
	if maxNeedMem > res.MemSwap+res.MemPhysical {
		log.Debugf("sched: not scheduling on worker %d; not enough virtual memory - need: %dM, have %dM", wid, maxNeedMem/mib, (res.MemSwap+res.MemPhysical)/mib)
		return false
	}

	if needRes.MultiThread() {
		if active.cpuUse > 0 {
			log.Debugf("sched: not scheduling on worker %d; multicore process needs %d threads, %d in use, target %d", wid, res.CPUs, active.cpuUse, res.CPUs)
			// return false
		}
	} else {
		if active.cpuUse+uint64(needRes.Threads) > res.CPUs {
			log.Debugf("sched: not scheduling on worker %d; not enough threads, need %d, %d in use, target %d", wid, needRes.Threads, active.cpuUse, res.CPUs)
			// return false
		}
	}

	if len(res.GPUs) > 0 && needRes.CanGPU {
		if active.gpuUsed {
			log.Debugf("sched: not scheduling on worker %d; GPU in use", wid)
			// return false
		}
	}

	return true
}

func (a *activeResources) utilization(wr storiface.WorkerResources) float64 {
	var max float64

	cpu := float64(a.cpuUse) / float64(wr.CPUs)
	max = cpu

	memMin := float64(a.memUsedMin+wr.MemReserved) / float64(wr.MemPhysical)
	if memMin > max {
		max = memMin
	}

	memMax := float64(a.memUsedMax+wr.MemReserved) / float64(wr.MemPhysical+wr.MemSwap)
	if memMax > max {
		max = memMax
	}

	return max
}

func (sh *scheduler) schedNewWorker(w *workerHandle) {
	sh.workersLk.Lock()

	id := sh.nextWorker
	sh.workers[id] = w
	sh.nextWorker++

	sh.workersLk.Unlock()

	select {
	case sh.watchClosing <- id:
	case <-sh.closing:
		return
	}

	sh.onWorkerFreed(id)
}

func (sh *scheduler) schedDropWorker(wid WorkerID) {
	sh.workersLk.Lock()
	defer sh.workersLk.Unlock()

	w := sh.workers[wid]
	delete(sh.workers, wid)

	go func() {
		if err := w.w.Close(); err != nil {
			log.Warnf("closing worker %d: %+v", err)
		}
	}()
}

func (sh *scheduler) schedClose() {
	sh.workersLk.Lock()
	defer sh.workersLk.Unlock()

	for i, w := range sh.workers {
		if err := w.w.Close(); err != nil {
			log.Errorf("closing worker %d: %+v", i, err)
		}
	}
}

func (sh *scheduler) Close() error {
	close(sh.closing)
	return nil
}

//===================添加的内容====================//
func (sh *scheduler) maybeSchedRequest(req *workerRequest) (bool, error) {
	sh.workersLk.Lock()
	defer sh.workersLk.Unlock()

	tried := 0
	var acceptable []WorkerID
	var freetable []int   // 添加的内容
	workerOnFree := false // 添加的内容
	best := 0             // 添加的内容
	for wid, worker := range sh.workers {
		ok, err := req.sel.Ok(req.ctx, req.taskType, sh.spt, worker)
		if err != nil {
			return false, err
		}

		if !ok {
			continue
		}
		tried++

		freecount := sh.canHandleRequestWithTaskCount(wid, req.taskType) // 修改的内容
		if freecount <= 0 {                                              // 修改的内容
			continue
		}
		freetable = append(freetable, freecount) // 修改的内容
		acceptable = append(acceptable, wid)

		if onfree := req.sel.FindDataWoker(req.ctx, req.taskType, req.sector, worker); onfree {
			workerOnFree = true
			break
		}
	}

	if len(acceptable) > 0 {
		if workerOnFree {
			best = len(acceptable) - 1
		} else {
			max := 0
			for i, v := range freetable {
				if v > max {
					max = v
					best = i
				}
			}
		}
		wid := acceptable[best]
		whl := sh.workers[wid]
		log.Infof("worker %s will be do the %+v jobTask!", whl.info.Hostname, req.taskType)
		return true, sh.assignWorker(wid, whl, req)
	}

	if tried == 0 {
		return false, xerrors.New("maybeSchedRequest didn't find any good workers")
	}
	return false, nil // put in waiting queue
}

func (sh *scheduler) taskAddOne(wid WorkerID, phaseTaskType sealtasks.TaskType) {
	if whl, ok := sh.workers[wid]; ok {
		whl.info.TaskResourcesLk.Lock()
		defer whl.info.TaskResourcesLk.Unlock()
		if counts, ok := whl.info.TaskResources[phaseTaskType]; ok {
			counts.RunCount++
		}
	}
}

func (sh *scheduler) taskReduceOne(wid WorkerID, phaseTaskType sealtasks.TaskType) {
	if whl, ok := sh.workers[wid]; ok {
		whl.info.TaskResourcesLk.Lock()
		defer whl.info.TaskResourcesLk.Unlock()
		if counts, ok := whl.info.TaskResources[phaseTaskType]; ok {
			counts.RunCount--
		}
	}
}

func (sh *scheduler) getTaskCount(wid WorkerID, phaseTaskType sealtasks.TaskType, typeCount string) int {
	if whl, ok := sh.workers[wid]; ok {
		if counts, ok := whl.info.TaskResources[phaseTaskType]; ok {
			whl.info.TaskResourcesLk.Lock()
			defer whl.info.TaskResourcesLk.Unlock()
			if typeCount == "limit" {
				return counts.LimitCount
			}
			if typeCount == "run" {
				return counts.RunCount
			}
		}
	}
	return 0
}

func (sh *scheduler) canHandleRequestWithTaskCount(wid WorkerID, phaseTaskType sealtasks.TaskType) int {
	// json文件限制的各阶段任务数量
	addPieceLimitCount := sh.getTaskCount(wid, sealtasks.TTAddPiece, "limit")
	p1LimitCount := sh.getTaskCount(wid, sealtasks.TTPreCommit1, "limit")
	p2LimitCount := sh.getTaskCount(wid, sealtasks.TTPreCommit2, "limit")
	c1LimitCount := sh.getTaskCount(wid, sealtasks.TTCommit1, "limit")
	c2LimitCount := sh.getTaskCount(wid, sealtasks.TTCommit2, "limit")
	fetchLimitCount := sh.getTaskCount(wid, sealtasks.TTFetch, "limit")
	finalizeLimitCount := sh.getTaskCount(wid, sealtasks.TTFinalize, "limit")

	// 运行中记录到的各阶段任务数量
	addPieceCurrentCount := sh.getTaskCount(wid, sealtasks.TTAddPiece, "run")
	p1CurrentCount := sh.getTaskCount(wid, sealtasks.TTPreCommit1, "run")
	p2CurrentCount := sh.getTaskCount(wid, sealtasks.TTPreCommit2, "run")
	c1CurrentCount := sh.getTaskCount(wid, sealtasks.TTCommit1, "run")
	c2CurrentCount := sh.getTaskCount(wid, sealtasks.TTCommit2, "run")
	// fetchCurrentCount := sh.getTaskCount(sealtasks.TTFetch, "run")
	// finalizeCurrentCount := sh.getTaskCount(sealtasks.TTFinalize, "run")

	whl := sh.workers[wid]
	a2Free := p1LimitCount - addPieceCurrentCount - p1CurrentCount
	b3Free := max(p2LimitCount, c1LimitCount, c2LimitCount) - p2CurrentCount - c1CurrentCount - c2CurrentCount
	log.Infof("worker %s: free task count {(addpiece+p1):%d (p2+c1+c2):%d}", whl.info.Hostname, a2Free, b3Free)

	if phaseTaskType == sealtasks.TTAddPiece {
		if addPieceLimitCount == 0 { // 限制数量为0，表示禁用此阶段的任务工作
			return 0
		}
		return a2Free // 返回前二个阶段的任务空闲数量
	}

	if phaseTaskType == sealtasks.TTPreCommit1 {
		if p1LimitCount == 0 {
			return 0
		}
		return a2Free // 返回前二个阶段的任务空闲数量
	}

	if phaseTaskType == sealtasks.TTPreCommit2 {
		if p2LimitCount == 0 {
			return 0
		}
		return b3Free // 返回后三个阶段的任务空闲数量
	}

	if phaseTaskType == sealtasks.TTCommit1 {
		if c1LimitCount == 0 {
			return 0
		}
		return b3Free // 返回后三个阶段的任务空闲数量
	}

	if phaseTaskType == sealtasks.TTCommit2 {
		if c2LimitCount == 0 {
			return 0
		}
		return b3Free // 返回后三个阶段的任务空闲数量
	}

	if phaseTaskType == sealtasks.TTFetch {
		if fetchLimitCount == 0 {
			return 0
		}
		return 1 // 这个阶段不应该限制，判断数据所在由本地去做
	}

	if phaseTaskType == sealtasks.TTFinalize {
		if finalizeLimitCount == 0 {
			return 0
		}
		return 1 // 这个阶段不应该限制，判断数据所在由本地去做
	}
	return 0
}

func max(a, b, c int) int {
	var max int

	if a > b {
		max = a
	} else {
		max = b
	}

	if max > c {
		return max
	}
	return c
}

//===================添加的内容====================//
