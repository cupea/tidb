// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"fmt"
	"math"

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/planner/util"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/paging"
	"github.com/pingcap/tidb/util/tracing"
	"github.com/pingcap/tipb/go-tipb"
)

// GetPlanCost returns the cost of this plan.
func GetPlanCost(p PhysicalPlan, taskType property.TaskType, option *PlanCostOption) (float64, error) {
	return getPlanCost(p, taskType, option)
}

func getPlanCost(p PhysicalPlan, taskType property.TaskType, option *PlanCostOption) (float64, error) {
	if p.SCtx().GetSessionVars().CostModelVersion == modelVer2 {
		planCost, err := p.getPlanCostVer2(taskType, option)
		return planCost.Cost, err
	}
	return p.getPlanCostVer1(taskType, option)
}

func getPlanCostDetail(p PhysicalPlan, ver2 CostVer2) *tracing.PhysicalPlanCostDetail {
	costDetail := tracing.NewPhysicalPlanCostDetail(p.ID(), p.TP())
	costDetail.SetDesc(ver2.Formula)
	costDetail.Cost = ver2.Cost
	/*
		for k, v := range ver2.CostParams {
			costDetail.AddParam(k, v)
		}
	*/
	return costDetail
}

// getPlanCostVer2 calculates the cost of the plan if it has not been calculated yet and returns the cost.
func (p *basePhysicalPlan) getPlanCostVer2(taskType property.TaskType, option *PlanCostOption) (CostVer2, error) {
	if p.planCostInit && !hasCostFlag(option.CostFlag, CostFlagRecalculate) {
		return p.planCostVer2, nil
	}
	childCosts := make([]CostVer2, 0, len(p.children))
	for _, child := range p.children {
		childCost, err := child.getPlanCostVer2(taskType, option)
		if err != nil {
			return zeroCostVer2, err
		}
		childCosts = append(childCosts, childCost)
	}
	if len(childCosts) == 0 {
		p.planCostVer2 = newZeroCostVer2(true)
	} else {
		p.planCostVer2 = sumCostVer2(childCosts...)
	}
	p.planCostInit = true
	if option.tracer != nil {
		option.tracer.appendPlanCostDetail(getPlanCostDetail(p, p.planCostVer2))
	}
	return p.planCostVer2, nil
}

func numFunctionsFactor(name string, exprs []expression.Expression) CostItem {
	num := 0.0
	for _, e := range exprs {
		if _, ok := e.(*expression.ScalarFunction); ok {
			num++
		} else { // Column and Constant
			num += 0.01 // an empirical value
		}
	}
	return newCostItem(name, num)
}

// getPlanCostVer2 returns the plan-cost of this sub-plan, which is:
// plan-cost = child-cost + filter-cost
func (p *PhysicalSelection) getPlanCostVer2(taskType property.TaskType, option *PlanCostOption) (CostVer2, error) {
	if p.planCostInit && !hasCostFlag(option.CostFlag, CostFlagRecalculate) {
		return p.planCostVer2, nil
	}
	inputRows := newCostItem("rows", getCardinality(p.children[0], option.CostFlag))
	builder := newCostBuilder(inputRows)
	cpuFactor := getTaskCPUFactorVer2(p, taskType)

	builder.mul(numFunctionsFactor("filter", p.Conditions)).mul(cpuFactor)

	childCost, err := p.children[0].getPlanCostVer2(taskType, option)
	if err != nil {
		return zeroCostVer2, err
	}

	builder.plus(childCost).setName(p.ExplainID().String())
	p.planCostVer2 = builder.Value()
	p.planCostInit = true
	return p.planCostVer2, nil
}

// getPlanCostVer2 returns the plan-cost of this sub-plan, which is:
// plan-cost = child-cost + proj-cost / concurrency
// proj-cost = input-rows * len(expressions) * cpu-factor
func (p *PhysicalProjection) getPlanCostVer2(taskType property.TaskType, option *PlanCostOption) (CostVer2, error) {
	if p.planCostInit && !hasCostFlag(option.CostFlag, CostFlagRecalculate) {
		return p.planCostVer2, nil
	}

	inputRows := newCostItem("rows", getCardinality(p.children[0], option.CostFlag))
	cpuFactor := getTaskCPUFactorVer2(p, taskType)
	concurrency := newCostItem("concurrency", float64(p.ctx.GetSessionVars().ProjectionConcurrency()))
	builder := newCostBuilder(inputRows)

	builder.mul(numFunctionsFactor("expr_count", p.Exprs)).mul(cpuFactor).div(concurrency)

	childCost, err := p.children[0].getPlanCostVer2(taskType, option)
	if err != nil {
		return zeroCostVer2, err
	}

	builder.plus(childCost).setName(p.ExplainID().String())
	p.planCostVer2 = builder.Value()
	p.planCostInit = true
	return p.planCostVer2, nil
}

// getPlanCostVer2 returns the plan-cost of this sub-plan, which is:
// plan-cost = rows * log2(row-size) * scan-factor
// log2(row-size) is from experiments.
func (p *PhysicalIndexScan) getPlanCostVer2(taskType property.TaskType, option *PlanCostOption) (CostVer2, error) {
	if p.planCostInit && !hasCostFlag(option.CostFlag, CostFlagRecalculate) {
		return p.planCostVer2, nil
	}

	rowSize := math.Max(getAvgRowSize(p.stats, p.schema.Columns), 2.0) // consider all index columns
	if rowSize < 1 {
		rowSize = 1
	}
	rowSizeCost := newCostItem("row_size", math.Log2(rowSize))
	inputRows := newCostItem("rows", getCardinality(p.children[0], option.CostFlag))
	builder := newCostBuilder(inputRows)
	scanFactor := getTaskScanFactorVer2(p, kv.TiKV, taskType)
	builder.mul(scanFactor).mul(rowSizeCost).setName(p.ExplainID().String())

	p.planCostVer2 = builder.Value()
	p.planCostInit = true
	return p.planCostVer2, nil
}

// getPlanCostVer2 returns the plan-cost of this sub-plan, which is:
// plan-cost = rows * log2(row-size) * scan-factor
// log2(row-size) is from experiments.
func (p *PhysicalTableScan) getPlanCostVer2(taskType property.TaskType, option *PlanCostOption) (CostVer2, error) {
	if p.planCostInit && !hasCostFlag(option.CostFlag, CostFlagRecalculate) {
		return p.planCostVer2, nil
	}

	rows := newCostItem("rows", getCardinality(p, option.CostFlag))
	var rowSize float64
	if p.StoreType == kv.TiKV {
		rowSize = getAvgRowSize(p.stats, p.tblCols) // consider all columns if TiKV
	} else { // TiFlash
		rowSize = getAvgRowSize(p.stats, p.schema.Columns)
	}
	rowSizeCost := newCostItem("log_row_size", math.Log2(math.Max(rowSize, 2.0)))
	scanFactor := getTaskScanFactorVer2(p, p.StoreType, taskType)
	builder := newCostBuilder(rows)
	builder.mul(rowSizeCost).mul(scanFactor)

	// give TiFlash a start-up cost to let the optimizer prefers to use TiKV to process small table scans.
	if p.StoreType == kv.TiFlash {
		nrows := newCostItem("10000", 10000)
		nbuilder := newCostBuilder(nrows)
		nbuilder.mul(rowSizeCost).mul(scanFactor)
		builder.plus(nbuilder.curr)
	}
	builder.setName(p.ExplainID().String())
	p.planCostVer2 = builder.Value()
	p.planCostInit = true
	return p.planCostVer2, nil
}

// getPlanCostVer2 returns the plan-cost of this sub-plan, which is:
// plan-cost = (child-cost + net-cost) / concurrency
// net-cost = rows * row-size * net-factor
func (p *PhysicalIndexReader) getPlanCostVer2(taskType property.TaskType, option *PlanCostOption) (CostVer2, error) {
	if p.planCostInit && !hasCostFlag(option.CostFlag, CostFlagRecalculate) {
		return p.planCostVer2, nil
	}

	rows := getCardinality(p.indexPlan, option.CostFlag)
	rowSize := getAvgRowSize(p.stats, p.schema.Columns)
	netFactor := getTaskNetFactorVer2(p, taskType)
	concurrency := float64(p.ctx.GetSessionVars().DistSQLScanConcurrency())

	netCost := netCostVer2(option, rows, rowSize, netFactor)

	childCost, err := p.indexPlan.getPlanCostVer2(property.CopSingleReadTaskType, option)
	if err != nil {
		return zeroCostVer2, err
	}

	p.planCostVer2 = divCostVer2(sumCostVer2(childCost, netCost), concurrency)
	p.planCostInit = true
	return p.planCostVer2, nil
}

// getPlanCostVer2 returns the plan-cost of this sub-plan, which is:
// plan-cost = (child-cost + net-cost) / concurrency
// net-cost = rows * row-size * net-factor
func (p *PhysicalTableReader) getPlanCostVer2(taskType property.TaskType, option *PlanCostOption) (CostVer2, error) {
	if p.planCostInit && !hasCostFlag(option.CostFlag, CostFlagRecalculate) {
		return p.planCostVer2, nil
	}

	rows := getCardinality(p.tablePlan, option.CostFlag)
	rowSize := getAvgRowSize(p.stats, p.schema.Columns)
	netFactor := getTaskNetFactorVer2(p, taskType)
	concurrency := float64(p.ctx.GetSessionVars().DistSQLScanConcurrency())
	childType := property.CopSingleReadTaskType
	if p.StoreType == kv.TiFlash { // mpp protocol
		childType = property.MppTaskType
	}

	netCost := netCostVer2(option, rows, rowSize, netFactor)

	childCost, err := p.tablePlan.getPlanCostVer2(childType, option)
	if err != nil {
		return zeroCostVer2, err
	}

	p.planCostVer2 = divCostVer2(sumCostVer2(childCost, netCost), concurrency)
	p.planCostInit = true

	// consider tidb_enforce_mpp
	if p.StoreType == kv.TiFlash && p.ctx.GetSessionVars().IsMPPEnforced() &&
		!hasCostFlag(option.CostFlag, CostFlagRecalculate) { // show the real cost in explain-statements
		p.planCostVer2 = divCostVer2(p.planCostVer2, 1000000000)
	}
	return p.planCostVer2, nil
}

// getPlanCostVer2 returns the plan-cost of this sub-plan, which is:
// plan-cost = index-side-cost + (table-side-cost + double-read-cost) / double-read-concurrency
// index-side-cost = (index-child-cost + index-net-cost) / dist-concurrency # same with IndexReader
// table-side-cost = (table-child-cost + table-net-cost) / dist-concurrency # same with TableReader
// double-read-cost = double-read-request-cost + double-read-cpu-cost
// double-read-request-cost = double-read-tasks * request-factor
// double-read-cpu-cost = index-rows * cpu-factor
// double-read-tasks = index-rows / batch-size * task-per-batch # task-per-batch is a magic number now
func (p *PhysicalIndexLookUpReader) getPlanCostVer2(taskType property.TaskType, option *PlanCostOption) (CostVer2, error) {
	if p.planCostInit && !hasCostFlag(option.CostFlag, CostFlagRecalculate) {
		return p.planCostVer2, nil
	}

	indexRows := getCardinality(p.indexPlan, option.CostFlag)
	tableRows := getCardinality(p.indexPlan, option.CostFlag)
	indexRowSize := getTblStats(p.indexPlan).GetAvgRowSize(p.ctx, p.indexPlan.Schema().Columns, true, false)
	tableRowSize := getTblStats(p.tablePlan).GetAvgRowSize(p.ctx, p.tablePlan.Schema().Columns, false, false)
	cpuFactor := getTaskCPUFactorVer2(p, taskType)
	netFactor := getTaskNetFactorVer2(p, taskType)
	requestFactor := getTaskRequestFactorVer2(p, taskType)
	distConcurrency := float64(p.ctx.GetSessionVars().DistSQLScanConcurrency())
	doubleReadConcurrency := float64(p.ctx.GetSessionVars().IndexLookupConcurrency())

	// index-side
	indexNetCost := netCostVer2(option, indexRows, indexRowSize, netFactor)
	indexChildCost, err := p.indexPlan.getPlanCostVer2(property.CopMultiReadTaskType, option)
	if err != nil {
		return zeroCostVer2, err
	}
	indexSideCost := divCostVer2(sumCostVer2(indexNetCost, indexChildCost), distConcurrency)

	// table-side
	tableNetCost := netCostVer2(option, tableRows, tableRowSize, netFactor)
	tableChildCost, err := p.tablePlan.getPlanCostVer2(property.CopMultiReadTaskType, option)
	if err != nil {
		return zeroCostVer2, err
	}
	tableSideCost := divCostVer2(sumCostVer2(tableNetCost, tableChildCost), distConcurrency)

	doubleReadRows := indexRows
	doubleReadCPUCost := newOldCostVer2(option, cpuFactor,
		indexRows*cpuFactor.Value,
		func() string { return fmt.Sprintf("double-read-cpu(%v*%v)", doubleReadRows, cpuFactor) })
	batchSize := float64(p.ctx.GetSessionVars().IndexLookupSize)
	taskPerBatch := 32.0 // TODO: remove this magic number
	doubleReadTasks := doubleReadRows / batchSize * taskPerBatch
	doubleReadRequestCost := doubleReadCostVer2(option, doubleReadTasks, requestFactor)
	doubleReadCost := sumCostVer2(doubleReadCPUCost, doubleReadRequestCost)

	p.planCostVer2 = sumCostVer2(indexSideCost, divCostVer2(sumCostVer2(tableSideCost, doubleReadCost), doubleReadConcurrency))

	if p.ctx.GetSessionVars().EnablePaging && p.expectedCnt > 0 && p.expectedCnt <= paging.Threshold {
		// if the expectCnt is below the paging threshold, using paging API
		p.Paging = true // TODO: move this operation from cost model to physical optimization
		p.planCostVer2 = mulCostVer2(p.planCostVer2, 0.6)
	}

	p.planCostInit = true
	return p.planCostVer2, nil
}

// getPlanCostVer2 returns the plan-cost of this sub-plan, which is:
// plan-cost = table-side-cost + sum(index-side-cost)
// index-side-cost = (index-child-cost + index-net-cost) / dist-concurrency # same with IndexReader
// table-side-cost = (table-child-cost + table-net-cost) / dist-concurrency # same with TableReader
func (p *PhysicalIndexMergeReader) getPlanCostVer2(taskType property.TaskType, option *PlanCostOption) (CostVer2, error) {
	if p.planCostInit && !hasCostFlag(option.CostFlag, CostFlagRecalculate) {
		return p.planCostVer2, nil
	}

	netFactor := getTaskNetFactorVer2(p, taskType)
	distConcurrency := float64(p.ctx.GetSessionVars().DistSQLScanConcurrency())

	var tableSideCost CostVer2
	if tablePath := p.tablePlan; tablePath != nil {
		rows := getCardinality(tablePath, option.CostFlag)
		rowSize := getAvgRowSize(tablePath.Stats(), tablePath.Schema().Columns)

		tableNetCost := netCostVer2(option, rows, rowSize, netFactor)
		tableChildCost, err := tablePath.getPlanCostVer2(taskType, option)
		if err != nil {
			return zeroCostVer2, err
		}
		tableSideCost = divCostVer2(sumCostVer2(tableNetCost, tableChildCost), distConcurrency)
	}

	indexSideCost := make([]CostVer2, 0, len(p.partialPlans))
	for _, indexPath := range p.partialPlans {
		rows := getCardinality(indexPath, option.CostFlag)
		rowSize := getAvgRowSize(indexPath.Stats(), indexPath.Schema().Columns)

		indexNetCost := netCostVer2(option, rows, rowSize, netFactor)
		indexChildCost, err := indexPath.getPlanCostVer2(taskType, option)
		if err != nil {
			return zeroCostVer2, err
		}
		indexSideCost = append(indexSideCost,
			divCostVer2(sumCostVer2(indexNetCost, indexChildCost), distConcurrency))
	}
	sumIndexSideCost := sumCostVer2(indexSideCost...)

	p.planCostVer2 = sumCostVer2(tableSideCost, sumIndexSideCost)
	p.planCostInit = true
	return p.planCostVer2, nil
}

// getPlanCostVer2 returns the plan-cost of this sub-plan, which is:
// plan-cost = child-cost + sort-cpu-cost + sort-mem-cost + sort-disk-cost
// sort-cpu-cost = rows * log2(rows) * len(sort-items) * cpu-factor
// if no spill:
// 1. sort-mem-cost = rows * row-size * mem-factor
// 2. sort-disk-cost = 0
// else if spill:
// 1. sort-mem-cost = mem-quota * mem-factor
// 2. sort-disk-cost = rows * row-size * disk-factor
func (p *PhysicalSort) getPlanCostVer2(taskType property.TaskType, option *PlanCostOption) (CostVer2, error) {
	if p.planCostInit && !hasCostFlag(option.CostFlag, CostFlagRecalculate) {
		return p.planCostVer2, nil
	}

	rows := math.Max(getCardinality(p.children[0], option.CostFlag), 1)
	rowSize := getAvgRowSize(p.statsInfo(), p.Schema().Columns)
	cpuFactor := getTaskCPUFactorVer2(p, taskType)
	memFactor := getTaskMemFactorVer2(p, taskType)
	diskFactor := defaultVer2Factors.TiDBDisk
	oomUseTmpStorage := variable.EnableTmpStorageOnOOM.Load()
	memQuota := p.ctx.GetSessionVars().MemTracker.GetBytesLimit()
	spill := taskType == property.RootTaskType && // only TiDB can spill
		oomUseTmpStorage && // spill is enabled
		memQuota > 0 && // mem-quota is set
		rowSize*rows > float64(memQuota) // exceed the mem-quota

	sortCPUCost := orderCostVer2(option, rows, rows, p.ByItems, cpuFactor)

	var sortMemCost, sortDiskCost CostVer2
	if !spill {
		sortMemCost = newOldCostVer2(option, memFactor,
			rows*rowSize*memFactor.Value,
			func() string { return fmt.Sprintf("sortMem(%v*%v*%v)", rows, rowSize, memFactor) })
		sortDiskCost = zeroCostVer2
	} else {
		sortMemCost = newOldCostVer2(option, memFactor,
			float64(memQuota)*memFactor.Value,
			func() string { return fmt.Sprintf("sortMem(%v*%v)", memQuota, memFactor) })
		sortDiskCost = newOldCostVer2(option, diskFactor,
			rows*rowSize*diskFactor.Value,
			func() string { return fmt.Sprintf("sortDisk(%v*%v*%v)", rows, rowSize, diskFactor) })
	}

	childCost, err := p.children[0].getPlanCostVer2(taskType, option)
	if err != nil {
		return zeroCostVer2, err
	}

	p.planCostVer2 = sumCostVer2(childCost, sortCPUCost, sortMemCost, sortDiskCost)
	p.planCostInit = true
	return p.planCostVer2, nil
}

// getPlanCostVer2 returns the plan-cost of this sub-plan, which is:
// plan-cost = child-cost + topn-cpu-cost + topn-mem-cost
// topn-cpu-cost = rows * log2(N) * len(sort-items) * cpu-factor
// topn-mem-cost = N * row-size * mem-factor
func (p *PhysicalTopN) getPlanCostVer2(taskType property.TaskType, option *PlanCostOption) (CostVer2, error) {
	if p.planCostInit && !hasCostFlag(option.CostFlag, CostFlagRecalculate) {
		return p.planCostVer2, nil
	}

	rows := getCardinality(p.children[0], option.CostFlag)
	N := math.Max(1, float64(p.Count+p.Offset))
	rowSize := getAvgRowSize(p.statsInfo(), p.Schema().Columns)
	cpuFactor := getTaskCPUFactorVer2(p, taskType)
	memFactor := getTaskMemFactorVer2(p, taskType)

	topNCPUCost := orderCostVer2(option, rows, N, p.ByItems, cpuFactor)
	topNMemCost := newOldCostVer2(option, memFactor,
		N*rowSize*memFactor.Value,
		func() string { return fmt.Sprintf("topMem(%v*%v*%v)", N, rowSize, memFactor) })

	childCost, err := p.children[0].getPlanCostVer2(taskType, option)
	if err != nil {
		return zeroCostVer2, err
	}

	p.planCostVer2 = sumCostVer2(childCost, topNCPUCost, topNMemCost)
	p.planCostInit = true
	return p.planCostVer2, nil
}

// getPlanCostVer2 returns the plan-cost of this sub-plan, which is:
// plan-cost = child-cost + agg-cost + group-cost
func (p *PhysicalStreamAgg) getPlanCostVer2(taskType property.TaskType, option *PlanCostOption) (CostVer2, error) {
	if p.planCostInit && !hasCostFlag(option.CostFlag, CostFlagRecalculate) {
		return p.planCostVer2, nil
	}

	rows := getCardinality(p.children[0], option.CostFlag)
	cpuFactor := getTaskCPUFactorVer2(p, taskType)

	aggCost := aggCostVer2(option, rows, p.AggFuncs, cpuFactor)
	groupCost := groupCostVer2(option, rows, p.GroupByItems, cpuFactor)

	childCost, err := p.children[0].getPlanCostVer2(taskType, option)
	if err != nil {
		return zeroCostVer2, err
	}

	p.planCostVer2 = sumCostVer2(childCost, aggCost, groupCost)
	p.planCostInit = true
	return p.planCostVer2, nil
}

// getPlanCostVer2 returns the plan-cost of this sub-plan, which is:
// plan-cost = child-cost + (agg-cost + group-cost + hash-build-cost + hash-probe-cost) / concurrency
func (p *PhysicalHashAgg) getPlanCostVer2(taskType property.TaskType, option *PlanCostOption) (CostVer2, error) {
	if p.planCostInit && !hasCostFlag(option.CostFlag, CostFlagRecalculate) {
		return p.planCostVer2, nil
	}

	inputRows := getCardinality(p.children[0], option.CostFlag)
	outputRows := getCardinality(p, option.CostFlag)
	outputRowSize := getAvgRowSize(p.Stats(), p.Schema().Columns)
	cpuFactor := getTaskCPUFactorVer2(p, taskType)
	memFactor := getTaskMemFactorVer2(p, taskType)
	concurrency := float64(p.ctx.GetSessionVars().HashAggFinalConcurrency())

	aggCost := aggCostVer2(option, inputRows, p.AggFuncs, cpuFactor)
	groupCost := groupCostVer2(option, inputRows, p.GroupByItems, cpuFactor)
	hashBuildCost := hashBuildCostVer2(option, outputRows, outputRowSize, float64(len(p.GroupByItems)), cpuFactor, memFactor)
	hashProbeCost := hashProbeCostVer2(option, inputRows, float64(len(p.GroupByItems)), cpuFactor)
	startCost := newOldCostVer2(option, cpuFactor,
		10*3*cpuFactor.Value, // 10rows * 3func * cpuFactor
		func() string { return fmt.Sprintf("cpu(10*3*%v)", cpuFactor) })

	childCost, err := p.children[0].getPlanCostVer2(taskType, option)
	if err != nil {
		return zeroCostVer2, err
	}

	p.planCostVer2 = sumCostVer2(startCost, childCost, divCostVer2(sumCostVer2(aggCost, groupCost, hashBuildCost, hashProbeCost), concurrency))
	p.planCostInit = true
	return p.planCostVer2, nil
}

// getPlanCostVer2 returns the plan-cost of this sub-plan, which is:
// plan-cost = left-child-cost + right-child-cost + filter-cost + group-cost
func (p *PhysicalMergeJoin) getPlanCostVer2(taskType property.TaskType, option *PlanCostOption) (CostVer2, error) {
	if p.planCostInit && !hasCostFlag(option.CostFlag, CostFlagRecalculate) {
		return p.planCostVer2, nil
	}

	leftRows := getCardinality(p.children[0], option.CostFlag)
	rightRows := getCardinality(p.children[1], option.CostFlag)
	cpuFactor := getTaskCPUFactorVer2(p, taskType)

	filterCost := sumCostVer2(filterCostVer2(option, leftRows, p.LeftConditions, cpuFactor),
		filterCostVer2(option, rightRows, p.RightConditions, cpuFactor))
	groupCost := sumCostVer2(groupCostVer2(option, leftRows, cols2Exprs(p.LeftJoinKeys), cpuFactor),
		groupCostVer2(option, rightRows, cols2Exprs(p.LeftJoinKeys), cpuFactor))

	leftChildCost, err := p.children[0].getPlanCostVer2(taskType, option)
	if err != nil {
		return zeroCostVer2, err
	}
	rightChildCost, err := p.children[1].getPlanCostVer2(taskType, option)
	if err != nil {
		return zeroCostVer2, err
	}

	p.planCostVer2 = sumCostVer2(leftChildCost, rightChildCost, filterCost, groupCost)
	p.planCostInit = true
	return p.planCostVer2, nil
}

// getPlanCostVer2 returns the plan-cost of this sub-plan, which is:
// plan-cost = build-child-cost + probe-child-cost +
// build-hash-cost + build-filter-cost +
// (probe-filter-cost + probe-hash-cost) / concurrency
func (p *PhysicalHashJoin) getPlanCostVer2(taskType property.TaskType, option *PlanCostOption) (CostVer2, error) {
	if p.planCostInit && !hasCostFlag(option.CostFlag, CostFlagRecalculate) {
		return p.planCostVer2, nil
	}

	build, probe := p.children[0], p.children[1]
	buildFilters, probeFilters := p.LeftConditions, p.RightConditions
	buildKeys, probeKeys := p.LeftJoinKeys, p.RightJoinKeys
	if (p.InnerChildIdx == 1 && !p.UseOuterToBuild) || (p.InnerChildIdx == 0 && p.UseOuterToBuild) {
		build, probe = probe, build
		buildFilters, probeFilters = probeFilters, buildFilters
	}
	buildRows := newCostItem("build_rows", getCardinality(build, option.CostFlag))
	probeRows := newCostItem("probe_rows", getCardinality(probe, option.CostFlag))
	buildRowSize := newCostItem("build_row_size", getAvgRowSize(build.Stats(), build.Schema().Columns))
	tidbConcurrency := newCostItem("tidb_concurrency", float64(p.Concurrency))
	mppConcurrency := newCostItem("mpp_concurrency", float64(3)) // TODO: remove this empirical value
	cpuFactor := getTaskCPUFactorVer2(p, taskType)
	memFactor := getTaskMemFactorVer2(p, taskType)

	builder := newCostBuilder(buildRows)
	builder.mul(cpuFactor).mul(newCostItem("build_filter_count", numFunctions(buildFilters))).setName("buildFilterCost")

	hashBuilder := newCostBuilder(buildRows)
	hashBuilder.mul(cpuFactor).mul(newCostItem("build_key_count", float64(len(buildKeys))))
	hashBuilder.plus(buildRows).mul(buildRowSize).mul(memFactor)
	hashBuilder.plus(buildRows).mul(cpuFactor).setName("buildHashCost")

	probeFilterBuilder := newCostBuilder(probeRows)
	probeFilterBuilder.mul(newCostItem("probe_filter_count", numFunctions(probeFilters))).mul(cpuFactor).setName("probeFilterCost")

	probeHashBuilder := newCostBuilder(probeRows)
	probeHashBuilder.mul(newCostItem("probe_key_count", float64(len(probeKeys)))).mul(cpuFactor).plus(probeRows).mul(cpuFactor).setName("probeHashCost")

	buildChildCost, err := build.getPlanCostVer2(taskType, option)
	if err != nil {
		return zeroCostVer2, err
	}
	probeChildCost, err := probe.getPlanCostVer2(taskType, option)
	if err != nil {
		return zeroCostVer2, err
	}

	if taskType == property.MppTaskType { // BCast or Shuffle Join, use mppConcurrency
		builder.sumAll(hashBuilder.curr, probeFilterBuilder.curr, probeHashBuilder.curr).divA(mppConcurrency).setName("probeCost").sumAll(buildChildCost, probeChildCost).setName(p.ExplainID().String())
		p.planCostVer2 = builder.Value()
	} else { // TiDB HashJoin
		probeFilterBuilder.plus(probeHashBuilder.curr).divA(tidbConcurrency).setName("probeCost")
		startCostBuilder := newCostBuilder(cpuFactor)
		startCostBuilder.mul(newCostItem("10", 10)).mul(newCostItem("3", 3)).setName("startCost")
		probeFilterBuilder.sumAll(startCostBuilder.curr, buildChildCost, probeChildCost, hashBuilder.curr, builder.curr).setName(p.ExplainID().String())
		p.planCostVer2 = probeFilterBuilder.Value()
	}
	p.planCostInit = true
	return p.planCostVer2, nil
}

func (p *PhysicalIndexJoin) getIndexJoinCostVer2(taskType property.TaskType, option *PlanCostOption, indexJoinType int) (CostVer2, error) {
	if p.planCostInit && !hasCostFlag(option.CostFlag, CostFlagRecalculate) {
		return p.planCostVer2, nil
	}

	build, probe := p.children[1-p.InnerChildIdx], p.children[p.InnerChildIdx]
	buildRows := getCardinality(build, option.CostFlag)
	buildRowSize := getAvgRowSize(build.Stats(), build.Schema().Columns)
	probeRowsOne := getCardinality(probe, option.CostFlag)
	probeRowsTot := probeRowsOne * buildRows
	probeRowSize := getAvgRowSize(probe.Stats(), probe.Schema().Columns)
	buildFilters, probeFilters := p.LeftConditions, p.RightConditions
	probeConcurrency := float64(p.ctx.GetSessionVars().IndexLookupJoinConcurrency())
	cpuFactor := getTaskCPUFactorVer2(p, taskType)
	memFactor := getTaskMemFactorVer2(p, taskType)
	requestFactor := getTaskRequestFactorVer2(p, taskType)

	buildFilterCost := filterCostVer2(option, buildRows, buildFilters, cpuFactor)
	buildChildCost, err := build.getPlanCostVer2(taskType, option)
	if err != nil {
		return zeroCostVer2, err
	}
	buildTaskCost := newOldCostVer2(option, cpuFactor,
		buildRows*10*cpuFactor.Value,
		func() string { return fmt.Sprintf("cpu(%v*10*%v)", buildRows, cpuFactor) })
	startCost := newOldCostVer2(option, cpuFactor,
		10*3*cpuFactor.Value,
		func() string { return fmt.Sprintf("cpu(10*3*%v)", cpuFactor) })

	probeFilterCost := filterCostVer2(option, probeRowsTot, probeFilters, cpuFactor)
	probeChildCost, err := probe.getPlanCostVer2(taskType, option)
	if err != nil {
		return zeroCostVer2, err
	}

	var hashTableCost CostVer2
	switch indexJoinType {
	case 1: // IndexHashJoin
		hashTableCost = hashBuildCostVer2(option, buildRows, buildRowSize, float64(len(p.RightJoinKeys)), cpuFactor, memFactor)
	case 2: // IndexMergeJoin
		hashTableCost = newZeroCostVer2(traceCost(option))
	default: // IndexJoin
		hashTableCost = hashBuildCostVer2(option, probeRowsTot, probeRowSize, float64(len(p.LeftJoinKeys)), cpuFactor, memFactor)
	}

	// IndexJoin executes a batch of rows at a time, so the actual cost of this part should be
	//  `innerCostPerBatch * numberOfBatches` instead of `innerCostPerRow * numberOfOuterRow`.
	// Use an empirical value batchRatio to handle this now.
	// TODO: remove this empirical value.
	batchRatio := 6.0
	probeCost := divCostVer2(mulCostVer2(probeChildCost, buildRows), batchRatio)

	// Double Read Cost
	doubleReadCost := newZeroCostVer2(traceCost(option))
	if p.ctx.GetSessionVars().IndexJoinDoubleReadPenaltyCostRate > 0 {
		batchSize := float64(p.ctx.GetSessionVars().IndexJoinBatchSize)
		taskPerBatch := 1024.0 // TODO: remove this magic number
		doubleReadTasks := buildRows / batchSize * taskPerBatch
		doubleReadCost = doubleReadCostVer2(option, doubleReadTasks, requestFactor)
		doubleReadCost = mulCostVer2(doubleReadCost, p.ctx.GetSessionVars().IndexJoinDoubleReadPenaltyCostRate)
	}

	p.planCostVer2 = sumCostVer2(startCost, buildChildCost, buildFilterCost, buildTaskCost, divCostVer2(sumCostVer2(doubleReadCost, probeCost, probeFilterCost, hashTableCost), probeConcurrency))
	p.planCostInit = true
	return p.planCostVer2, nil
}

// getPlanCostVer2 returns the plan-cost of this sub-plan, which is:
// plan-cost = build-child-cost + build-filter-cost +
// (probe-cost + probe-filter-cost) / concurrency
// probe-cost = probe-child-cost * build-rows / batchRatio
func (p *PhysicalIndexJoin) getPlanCostVer2(taskType property.TaskType, option *PlanCostOption) (CostVer2, error) {
	return p.getIndexJoinCostVer2(taskType, option, 0)
}

func (p *PhysicalIndexHashJoin) getPlanCostVer2(taskType property.TaskType, option *PlanCostOption) (CostVer2, error) {
	return p.getIndexJoinCostVer2(taskType, option, 1)
}

func (p *PhysicalIndexMergeJoin) getPlanCostVer2(taskType property.TaskType, option *PlanCostOption) (CostVer2, error) {
	return p.getIndexJoinCostVer2(taskType, option, 2)
}

// getPlanCostVer2 returns the plan-cost of this sub-plan, which is:
// plan-cost = build-child-cost + build-filter-cost + probe-cost + probe-filter-cost
// probe-cost = probe-child-cost * build-rows
func (p *PhysicalApply) getPlanCostVer2(taskType property.TaskType, option *PlanCostOption) (CostVer2, error) {
	if p.planCostInit && !hasCostFlag(option.CostFlag, CostFlagRecalculate) {
		return p.planCostVer2, nil
	}

	buildRows := getCardinality(p.children[0], option.CostFlag)
	probeRowsOne := getCardinality(p.children[1], option.CostFlag)
	probeRowsTot := buildRows * probeRowsOne
	cpuFactor := getTaskCPUFactorVer2(p, taskType)

	buildFilterCost := filterCostVer2(option, buildRows, p.LeftConditions, cpuFactor)
	buildChildCost, err := p.children[0].getPlanCostVer2(taskType, option)
	if err != nil {
		return zeroCostVer2, err
	}

	probeFilterCost := filterCostVer2(option, probeRowsTot, p.RightConditions, cpuFactor)
	probeChildCost, err := p.children[1].getPlanCostVer2(taskType, option)
	if err != nil {
		return zeroCostVer2, err
	}
	probeCost := mulCostVer2(probeChildCost, buildRows)

	p.planCostVer2 = sumCostVer2(buildChildCost, buildFilterCost, probeCost, probeFilterCost)
	p.planCostInit = true
	return p.planCostVer2, nil
}

// getPlanCostVer2 calculates the cost of the plan if it has not been calculated yet and returns the cost.
// plan-cost = sum(child-cost) / concurrency
func (p *PhysicalUnionAll) getPlanCostVer2(taskType property.TaskType, option *PlanCostOption) (CostVer2, error) {
	if p.planCostInit && !hasCostFlag(option.CostFlag, CostFlagRecalculate) {
		return p.planCostVer2, nil
	}

	concurrency := float64(p.ctx.GetSessionVars().UnionConcurrency())
	childCosts := make([]CostVer2, 0, len(p.children))
	for _, child := range p.children {
		childCost, err := child.getPlanCostVer2(taskType, option)
		if err != nil {
			return zeroCostVer2, err
		}
		childCosts = append(childCosts, childCost)
	}
	p.planCostVer2 = divCostVer2(sumCostVer2(childCosts...), concurrency)
	p.planCostInit = true
	return p.planCostVer2, nil
}

// getPlanCostVer2 returns the plan-cost of this sub-plan, which is:
// plan-cost = child-cost + net-cost
func (p *PhysicalExchangeReceiver) getPlanCostVer2(taskType property.TaskType, option *PlanCostOption) (CostVer2, error) {
	if p.planCostInit && !hasCostFlag(option.CostFlag, CostFlagRecalculate) {
		return p.planCostVer2, nil
	}

	rows := getCardinality(p, option.CostFlag)
	rowSize := getAvgRowSize(p.stats, p.Schema().Columns)
	netFactor := getTaskNetFactorVer2(p, taskType)
	isBCast := false
	if sender, ok := p.children[0].(*PhysicalExchangeSender); ok {
		isBCast = sender.ExchangeType == tipb.ExchangeType_Broadcast
	}
	numNode := float64(3) // TODO: remove this empirical value

	netCost := netCostVer2(option, rows, rowSize, netFactor)
	if isBCast {
		netCost = mulCostVer2(netCost, numNode)
	}
	childCost, err := p.children[0].getPlanCostVer2(taskType, option)
	if err != nil {
		return zeroCostVer2, err
	}

	p.planCostVer2 = sumCostVer2(childCost, netCost)
	p.planCostInit = true
	return p.planCostVer2, nil
}

// getPlanCostVer2 returns the plan-cost of this sub-plan, which is:
func (p *PointGetPlan) getPlanCostVer2(taskType property.TaskType, option *PlanCostOption) (CostVer2, error) {
	if p.planCostInit && !hasCostFlag(option.CostFlag, CostFlagRecalculate) {
		return p.planCostVer2, nil
	}

	if p.accessCols == nil { // from fast plan code path
		p.planCostVer2 = zeroCostVer2
		p.planCostInit = true
		return zeroCostVer2, nil
	}
	rowSize := getAvgRowSize(p.stats, p.schema.Columns)
	netFactor := getTaskNetFactorVer2(p, taskType)

	p.planCostVer2 = netCostVer2(option, 1, rowSize, netFactor)
	p.planCostInit = true
	return p.planCostVer2, nil
}

// getPlanCostVer2 returns the plan-cost of this sub-plan, which is:
func (p *BatchPointGetPlan) getPlanCostVer2(taskType property.TaskType, option *PlanCostOption) (CostVer2, error) {
	if p.planCostInit && !hasCostFlag(option.CostFlag, CostFlagRecalculate) {
		return p.planCostVer2, nil
	}

	if p.accessCols == nil { // from fast plan code path
		p.planCostVer2 = zeroCostVer2
		p.planCostInit = true
		return zeroCostVer2, nil
	}
	rows := getCardinality(p, option.CostFlag)
	rowSize := getAvgRowSize(p.stats, p.schema.Columns)
	netFactor := getTaskNetFactorVer2(p, taskType)

	p.planCostVer2 = netCostVer2(option, rows, rowSize, netFactor)
	p.planCostInit = true
	return p.planCostVer2, nil
}

func scanCostVer2(option *PlanCostOption, rows, rowSize float64, scanFactor CostVer2Factor) CostVer2 {
	if rowSize < 1 {
		rowSize = 1
	}
	return newOldCostVer2(option, scanFactor,
		// rows * log(row-size) * scanFactor, log2 from experiments
		rows*math.Log2(rowSize)*scanFactor.Value,
		func() string { return fmt.Sprintf("scan(%v*logrowsize(%v)*%v)", rows, rowSize, scanFactor) })
}

func netCostVer2(option *PlanCostOption, rows, rowSize float64, netFactor CostVer2Factor) CostVer2 {
	return newOldCostVer2(option, netFactor,
		rows*rowSize*netFactor.Value,
		func() string { return fmt.Sprintf("net(%v*rowsize(%v)*%v)", rows, rowSize, netFactor) })
}

func filterCostVer2(option *PlanCostOption, rows float64, filters []expression.Expression, cpuFactor CostVer2Factor) CostVer2 {
	numFuncs := numFunctions(filters)
	return newOldCostVer2(option, cpuFactor,
		rows*numFuncs*cpuFactor.Value,
		func() string { return fmt.Sprintf("cpu(%v*filters(%v)*%v)", rows, numFuncs, cpuFactor) })
}

func aggCostVer2(option *PlanCostOption, rows float64, aggFuncs []*aggregation.AggFuncDesc, cpuFactor CostVer2Factor) CostVer2 {
	return newOldCostVer2(option, cpuFactor,
		// TODO: consider types of agg-funcs
		rows*float64(len(aggFuncs))*cpuFactor.Value,
		func() string { return fmt.Sprintf("agg(%v*aggs(%v)*%v)", rows, len(aggFuncs), cpuFactor) })
}

func groupCostVer2(option *PlanCostOption, rows float64, groupItems []expression.Expression, cpuFactor CostVer2Factor) CostVer2 {
	numFuncs := numFunctions(groupItems)
	return newOldCostVer2(option, cpuFactor,
		rows*numFuncs*cpuFactor.Value,
		func() string { return fmt.Sprintf("group(%v*cols(%v)*%v)", rows, numFuncs, cpuFactor) })
}

func numFunctions(exprs []expression.Expression) float64 {
	num := 0.0
	for _, e := range exprs {
		if _, ok := e.(*expression.ScalarFunction); ok {
			num++
		} else { // Column and Constant
			num += 0.01 // an empirical value
		}
	}
	return num
}

func orderCostVer2(option *PlanCostOption, rows, N float64, byItems []*util.ByItems, cpuFactor CostVer2Factor) CostVer2 {
	numFuncs := 0
	for _, byItem := range byItems {
		if _, ok := byItem.Expr.(*expression.ScalarFunction); ok {
			numFuncs++
		}
	}
	exprCost := newOldCostVer2(option, cpuFactor,
		rows*float64(numFuncs)*cpuFactor.Value,
		func() string { return fmt.Sprintf("exprCPU(%v*%v*%v)", rows, numFuncs, cpuFactor) })
	orderCost := newOldCostVer2(option, cpuFactor,
		rows*math.Log2(N)*cpuFactor.Value,
		func() string { return fmt.Sprintf("orderCPU(%v*log(%v)*%v)", rows, N, cpuFactor) })
	return sumCostVer2(exprCost, orderCost)
}

func hashBuildCostVer2(option *PlanCostOption, buildRows, buildRowSize, nKeys float64, cpuFactor, memFactor CostVer2Factor) CostVer2 {
	// TODO: 1) consider types of keys, 2) dedicated factor for build-probe hash table
	hashKeyCost := newOldCostVer2(option, cpuFactor,
		buildRows*nKeys*cpuFactor.Value,
		func() string { return fmt.Sprintf("hashkey(%v*%v*%v)", buildRows, nKeys, cpuFactor) })
	hashMemCost := newOldCostVer2(option, memFactor,
		buildRows*buildRowSize*memFactor.Value,
		func() string { return fmt.Sprintf("hashmem(%v*%v*%v)", buildRows, buildRowSize, memFactor) })
	hashBuildCost := newOldCostVer2(option, cpuFactor,
		buildRows*cpuFactor.Value,
		func() string { return fmt.Sprintf("hashbuild(%v*%v)", buildRows, cpuFactor) })
	return sumCostVer2(hashKeyCost, hashMemCost, hashBuildCost)
}

func hashProbeCostVer2(option *PlanCostOption, probeRows, nKeys float64, cpuFactor CostVer2Factor) CostVer2 {
	// TODO: 1) consider types of keys, 2) dedicated factor for build-probe hash table
	hashKeyCost := newOldCostVer2(option, cpuFactor,
		probeRows*nKeys*cpuFactor.Value,
		func() string { return fmt.Sprintf("hashkey(%v*%v*%v)", probeRows, nKeys, cpuFactor) })
	hashProbeCost := newOldCostVer2(option, cpuFactor,
		probeRows*cpuFactor.Value,
		func() string { return fmt.Sprintf("hashprobe(%v*%v)", probeRows, cpuFactor) })
	return sumCostVer2(hashKeyCost, hashProbeCost)
}

// For simplicity and robust, only operators that need double-read like IndexLookup and IndexJoin consider this cost.
func doubleReadCostVer2(option *PlanCostOption, numTasks float64, requestFactor CostVer2Factor) CostVer2 {
	return newOldCostVer2(option, requestFactor,
		numTasks*requestFactor.Value,
		func() string { return fmt.Sprintf("doubleRead(tasks(%v)*%v)", numTasks, requestFactor) })
}

type CostItem interface {
	IsConst() bool
	GetCost() float64
	AsFormula() string
	GetName() string
}

type CostVer2Factor struct {
	Name  string
	Value float64
}

func (CostVer2Factor) IsConst() bool {
	return true
}

func (f CostVer2Factor) GetCost() float64 {
	return f.Value
}

func (f CostVer2Factor) AsFormula() string {
	return f.Name
}

func (f CostVer2Factor) GetName() string {
	return f.Name
}

func (f CostVer2Factor) String() string {
	return fmt.Sprintf("%s(%v)", f.Name, f.Value)
}

// In Cost Ver2, we hide cost factors from users and deprecate SQL variables like `tidb_opt_scan_factor`.
type CostVer2Factors struct {
	TiDBTemp      CostVer2Factor // operations on TiDB temporary table
	TiKVScan      CostVer2Factor // per byte
	TiKVDescScan  CostVer2Factor // per byte
	TiFlashScan   CostVer2Factor // per byte
	TiDBCPU       CostVer2Factor // per column or expression
	TiKVCPU       CostVer2Factor // per column or expression
	TiFlashCPU    CostVer2Factor // per column or expression
	TiDB2KVNet    CostVer2Factor // per byte
	TiDB2FlashNet CostVer2Factor // per byte
	TiFlashMPPNet CostVer2Factor // per byte
	TiDBMem       CostVer2Factor // per byte
	TiKVMem       CostVer2Factor // per byte
	TiFlashMem    CostVer2Factor // per byte
	TiDBDisk      CostVer2Factor // per byte
	TiDBRequest   CostVer2Factor // per net request
}

func (c CostVer2Factors) tolist() (l []CostVer2Factor) {
	return append(l, c.TiDBTemp, c.TiKVScan, c.TiKVDescScan, c.TiFlashScan, c.TiDBCPU, c.TiKVCPU, c.TiFlashCPU,
		c.TiDB2KVNet, c.TiDB2FlashNet, c.TiFlashMPPNet, c.TiDBMem, c.TiKVMem, c.TiFlashMem, c.TiDBDisk, c.TiDBRequest)
}

var constFactors map[string]float64

const (
	TiDBTemp      = "tidb_temp_table_factor"
	TiKVScan      = "tikv_scan_factor"
	TiKVDescScan  = "tikv_desc_scan_factor"
	TiFlashScan   = "tiflash_scan_factor"
	TiDBCPU       = "tidb_cpu_factor"
	TiKVCPU       = "tikv_cpu_factor"
	TiFlashCPU    = "tiflash_cpu_factor"
	TiDB2KVNet    = "tidb_kv_net_factor"
	TiDB2FlashNet = "tidb_flash_net_factor"
	TiFlashMPPNet = "tiflash_mpp_net_factor"
	TiDBMem       = "tidb_mem_factor"
	TiKVMem       = "tikv_mem_factor"
	TiFlashMem    = "tiflash_mem_factor"
	TiDBDisk      = "tidb_disk_factor"
	TiDBRequest   = "tidb_request_factor"
)

func initFactors() {
	constFactors = make(map[string]float64, 20)
	constFactors[TiDBTemp] = 0.00
	constFactors[TiKVScan] = 40.70
	constFactors[TiKVDescScan] = 61.05
	constFactors[TiFlashScan] = 11.60
	constFactors[TiDBCPU] = 49.90
	constFactors[TiKVCPU] = 49.90
	constFactors[TiFlashCPU] = 2.40
	constFactors[TiDB2KVNet] = 3.96
	constFactors[TiDB2FlashNet] = 2.20
	constFactors[TiFlashMPPNet] = 1.00
	constFactors[TiDBMem] = 0.20
	constFactors[TiKVMem] = 0.20
	constFactors[TiFlashMem] = 0.05
	constFactors[TiDBDisk] = 200.00
	constFactors[TiDBRequest] = 6000000.00
}

var defaultVer2Factors = CostVer2Factors{
	TiDBTemp:      CostVer2Factor{"tidb_temp_table_factor", 0.00},
	TiKVScan:      CostVer2Factor{"tikv_scan_factor", 40.70},
	TiKVDescScan:  CostVer2Factor{"tikv_desc_scan_factor", 61.05},
	TiFlashScan:   CostVer2Factor{"tiflash_scan_factor", 11.60},
	TiDBCPU:       CostVer2Factor{"tidb_cpu_factor", 49.90},
	TiKVCPU:       CostVer2Factor{"tikv_cpu_factor", 49.90},
	TiFlashCPU:    CostVer2Factor{"tiflash_cpu_factor", 2.40},
	TiDB2KVNet:    CostVer2Factor{"tidb_kv_net_factor", 3.96},
	TiDB2FlashNet: CostVer2Factor{"tidb_flash_net_factor", 2.20},
	TiFlashMPPNet: CostVer2Factor{"tiflash_mpp_net_factor", 1.00},
	TiDBMem:       CostVer2Factor{"tidb_mem_factor", 0.20},
	TiKVMem:       CostVer2Factor{"tikv_mem_factor", 0.20},
	TiFlashMem:    CostVer2Factor{"tiflash_mem_factor", 0.05},
	TiDBDisk:      CostVer2Factor{"tidb_disk_factor", 200.00},
	TiDBRequest:   CostVer2Factor{"tidb_request_factor", 6000000.00},
}

func getTaskCPUFactorVer2(p PhysicalPlan, taskType property.TaskType) CostVer2Factor {
	switch taskType {
	case property.RootTaskType: // TiDB
		return defaultVer2Factors.TiDBCPU
	case property.MppTaskType: // TiFlash
		return defaultVer2Factors.TiFlashCPU
	default: // TiKV
		return defaultVer2Factors.TiKVCPU
	}
}

func getTaskMemFactorVer2(p PhysicalPlan, taskType property.TaskType) CostVer2Factor {
	switch taskType {
	case property.RootTaskType: // TiDB
		return defaultVer2Factors.TiDBMem
	case property.MppTaskType: // TiFlash
		return defaultVer2Factors.TiFlashMem
	default: // TiKV
		return defaultVer2Factors.TiKVMem
	}
}

func getTaskScanFactorVer2(p PhysicalPlan, storeType kv.StoreType, taskType property.TaskType) CostVer2Factor {
	if isTemporaryTable(getTableInfo(p)) {
		return defaultVer2Factors.TiDBTemp
	}
	if storeType == kv.TiFlash {
		return defaultVer2Factors.TiFlashScan
	}
	switch taskType {
	case property.MppTaskType: // TiFlash
		return defaultVer2Factors.TiFlashScan
	default: // TiKV
		var desc bool
		if indexScan, ok := p.(*PhysicalIndexScan); ok {
			desc = indexScan.Desc
		}
		if tableScan, ok := p.(*PhysicalTableScan); ok {
			desc = tableScan.Desc
		}
		if desc {
			return defaultVer2Factors.TiKVDescScan
		}
		return defaultVer2Factors.TiKVScan
	}
}

func getTaskNetFactorVer2(p PhysicalPlan, _ property.TaskType) CostVer2Factor {
	if isTemporaryTable(getTableInfo(p)) {
		return defaultVer2Factors.TiDBTemp
	}
	if _, ok := p.(*PhysicalExchangeReceiver); ok { // TiFlash MPP
		return defaultVer2Factors.TiFlashMPPNet
	}
	if tblReader, ok := p.(*PhysicalTableReader); ok {
		if _, isMPP := tblReader.tablePlan.(*PhysicalExchangeSender); isMPP { // TiDB to TiFlash with mpp protocol
			return defaultVer2Factors.TiDB2FlashNet
		}
	}
	return defaultVer2Factors.TiDB2KVNet
}

func getTaskRequestFactorVer2(p PhysicalPlan, _ property.TaskType) CostVer2Factor {
	if isTemporaryTable(getTableInfo(p)) {
		return defaultVer2Factors.TiDBTemp
	}
	return defaultVer2Factors.TiDBRequest
}

func isTemporaryTable(tbl *model.TableInfo) bool {
	return tbl != nil && tbl.TempTableType != model.TempTableNone
}

func getTableInfo(p PhysicalPlan) *model.TableInfo {
	switch x := p.(type) {
	case *PhysicalIndexReader:
		return getTableInfo(x.indexPlan)
	case *PhysicalTableReader:
		return getTableInfo(x.tablePlan)
	case *PhysicalIndexLookUpReader:
		return getTableInfo(x.tablePlan)
	case *PhysicalIndexMergeReader:
		if x.tablePlan != nil {
			return getTableInfo(x.tablePlan)
		}
		return getTableInfo(x.partialPlans[0])
	case *PhysicalTableScan:
		return x.Table
	case *PhysicalIndexScan:
		return x.Table
	default:
		if len(x.Children()) == 0 {
			return nil
		}
		return getTableInfo(x.Children()[0])
	}
}

func cols2Exprs(cols []*expression.Column) []expression.Expression {
	exprs := make([]expression.Expression, 0, len(cols))
	for _, c := range cols {
		exprs = append(exprs, c)
	}
	return exprs
}

func traceCost(option *PlanCostOption) bool {
	if option != nil && hasCostFlag(option.CostFlag, CostFlagTrace) {
		return true
	}
	return false
}

func newZeroCostVer2(_ bool) (ret CostVer2) {
	return
}

type CostVer2 struct {
	Name    string
	Cost    float64
	Formula string // It used to trace the cost calculation.
	factors map[string]CostItem
}

func (CostVer2) IsConst() bool {
	return false
}

func (f *CostVer2) SetCost(v float64) {
	f.Cost = v
}

func (f CostVer2) GetCost() float64 {
	return f.Cost
}

func (f CostVer2) GetName() string {
	return f.Name
}

func (c CostVer2) AsFormula() string {
	if len(c.Name) > 0 {
		return c.Name
	}
	return c.Formula
}

func (c *CostVer2) SetName(name string) {
	c.Name = name
}

func GetCostItem(name string, cost float64) CostItem {
	return newCostItem(name, cost)
}

func newCostItem(name string, cost float64) CostItem {
	return CostVer2Factor{Name: name, Value: cost}
}

func newCostVer2(formula string, cost float64) CostVer2 {
	return CostVer2{Formula: formula, Cost: cost, factors: make(map[string]CostItem)}
}

const (
	PLUS = '+'
	SUB  = '-'
	MUL  = '*'
	DIV  = '/'
	MULA = '0'
	DIVA = '1'
)

type CostBuilder struct {
	curr   *CostVer2 //point to new allocated CostVer2 during calculation, for name/cost/formula updating
	init   CostItem  //save the init value, mainly for CostVer2Factor
	lastV  float64
	lastOp byte
}

// for test only
func GetCostBuilderForTest(init CostItem) *CostBuilder {
	return &CostBuilder{
		init: init,
		curr: nil,
	}
}

// for test only
func (builder *CostBuilder) EvalOpForTest(op string, v CostItem) {
	switch op {
	case "+":
		builder.plus(v)
	case "-":
		builder.sub(v)
	case "*":
		builder.mul(v)
	case "/":
		builder.div(v)
	case ")*":
		builder.mulA(v)
	case ")/":
		builder.divA(v)
	}
}

// for test only
func (builder *CostBuilder) SetNameForTest(name string) *CostBuilder {
	return builder.setName(name)
}

func newCostBuilder(init CostItem) CostBuilder {
	return CostBuilder{
		init: init,
		curr: nil,
	}
}

func (builder *CostBuilder) Value() CostVer2 {
	return *builder.curr
}

func (builder *CostBuilder) setName(name string) *CostBuilder {
	builder.curr.SetName(name)
	return builder
}

func (builder *CostBuilder) calcCost(curOp byte, cost float64) float64 {
	var curCost float64
	if builder.curr != nil {
		curCost = builder.curr.GetCost()
	} else {
		curCost = builder.init.GetCost()
	}
	switch curOp {
	case PLUS:
		builder.lastV = cost
		builder.lastOp = curOp
		return curCost + cost
	case SUB:
		builder.lastV = cost
		builder.lastOp = curOp
		return curCost - cost
	case MULA:
		builder.lastV = cost
		builder.lastOp = curOp
		return curCost * cost
	case DIVA:
		builder.lastV = cost
		builder.lastOp = curOp
		return curCost / cost
	}

	switch builder.lastOp {
	case PLUS:
		curCost -= builder.lastV
		if curOp == MUL {
			builder.lastV *= cost
		} else if curOp == DIV {
			builder.lastV /= cost
		}
		curCost += builder.lastV
	case SUB:
		curCost += builder.lastV
		if curOp == MUL {
			builder.lastV *= cost
		} else if curOp == DIV {
			builder.lastV /= cost
		}
		curCost -= builder.lastV
	default:
		if curOp == MUL {
			curCost *= cost
		} else if curOp == DIV {
			curCost /= cost
		}
	}
	return curCost
}

func isAnnoymous(v CostItem) bool {
	return !v.IsConst() && len(v.GetName()) == 0
}

func (builder *CostBuilder) opFinish(op byte, v CostItem, cost float64) {
	var curFormula string
	var formula string
	vformula := v.AsFormula()
	if builder.curr != nil {
		curFormula = builder.curr.AsFormula()
	} else {
		curFormula = builder.init.AsFormula()
	}
	//if annoymous CostVer2, should add "()", otherwise formula is incorrect
	if isAnnoymous(v) {
		vformula = "( " + vformula + " )"
	}
	switch op {
	case MULA:
		formula = "( " + curFormula + " )* " + vformula
	case DIVA:
		formula = "( " + curFormula + " )/ " + vformula
	default:
		formula = curFormula + " " + string(op) + " " + vformula
	}
	// if v is constVer2, allocate new one as parent
	if !v.IsConst() {
		newCurr := newCostVer2(formula, cost)
		//for CostVer2Factor or named CostVer2, we should add itself to factors,
		//otherwise it's annoymous CostVer2, we should move it's params to the newCurr
		if len(v.GetName()) > 0 {
			newCurr.factors[v.AsFormula()] = v
		} else {
			cv := v.(CostVer2)
			for k, v := range cv.factors {
				newCurr.factors[k] = v
			}
		}
		if builder.curr != nil {
			//should handle builder.curr too, same logic with previous
			if len(builder.curr.GetName()) > 0 {
				newCurr.factors[builder.curr.AsFormula()] = *builder.curr
			} else {
				for k, v := range builder.curr.factors {
					newCurr.factors[k] = v
				}
			}
		} else {
			newCurr.factors[builder.init.AsFormula()] = builder.init
		}

		builder.curr = &newCurr
	} else {
		//if v is a cost item, only add to factors

		//if builder.curr is null, create new costver2 to store factors
		if builder.curr == nil {
			newCurr := newCostVer2(formula, cost)
			//add init to factors
			newCurr.factors[builder.init.AsFormula()] = builder.init
			builder.curr = &newCurr
		} else {
			//reuse curr, just update formula and cost
			builder.curr.Formula = formula
			builder.curr.Cost = cost
		}
		builder.curr.factors[v.AsFormula()] = v
	}
}

func (builder *CostBuilder) sumAll(params ...CostItem) *CostBuilder {
	for _, param := range params {
		builder.plus(param)
	}
	return builder
}

func (builder *CostBuilder) plus(v CostItem) *CostBuilder {
	cost := builder.calcCost(PLUS, v.GetCost())
	builder.opFinish(PLUS, v, cost)
	return builder
}

func (builder *CostBuilder) sub(v CostItem) *CostBuilder {
	cost := builder.calcCost(SUB, v.GetCost())
	builder.opFinish(SUB, v, cost)
	return builder
}

func (builder *CostBuilder) div(v CostItem) *CostBuilder {
	cost := builder.calcCost(DIV, v.GetCost())
	builder.opFinish(DIV, v, cost)
	return builder
}

func (builder *CostBuilder) mul(v CostItem) *CostBuilder {
	cost := builder.calcCost(MUL, v.GetCost())
	builder.opFinish(MUL, v, cost)
	return builder
}

func (builder *CostBuilder) mulA(v CostItem) *CostBuilder {
	cost := builder.calcCost(MULA, v.GetCost())
	builder.opFinish(MULA, v, cost)
	return builder
}

func (builder *CostBuilder) divA(v CostItem) *CostBuilder {
	cost := builder.calcCost(DIVA, v.GetCost())
	builder.opFinish(DIVA, v, cost)
	return builder
}

func sumCostVer2(costs ...CostVer2) (ret CostVer2) {
	return
}

func divCostVer2(cost CostVer2, denominator float64) (ret CostVer2) {
	return
}

func mulCostVer2(Cost CostVer2, scale float64) (ret CostVer2) {
	return
}

func newOldCostVer2(option *PlanCostOption, factor CostVer2Factor, cost float64, lazyFormula func() string) (ret CostVer2) {
	ret.Cost = cost
	/*
		if traceCost(option) {
			ret.trace = &costTrace{make(map[string]float64), ""}
			ret.trace.factorCosts[factor.Name] = cost
			ret.trace.Formula = lazyFormula()
		}
	*/
	return ret
}

var zeroCostVer2 = newZeroCostVer2(false)
