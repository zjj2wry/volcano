/*
Copyright 2025 The Volcano Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package gpumanager

import (
	"strconv"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager/bitmask"
	"k8s.io/utils/cpuset"

	nodeinfov1alpha1 "volcano.sh/apis/pkg/apis/nodeinfo/v1alpha1"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/plugins/numaaware/policy"
)

const (
	// GPUResourceName is the standard GPU resource name
	GPUResourceName = "nvidia.com/gpu"
)

type gpuManager struct{}

// NewProvider returns a new GPU HintProvider
func NewProvider() policy.HintProvider {
	return &gpuManager{}
}

// Name returns the GPU manager name
func (gm *gpuManager) Name() string {
	return "gpuManager"
}

// guaranteedGPUs returns the integer number of requested GPUs
func guaranteedGPUs(container *v1.Container) int {
	gpuQuantity, ok := container.Resources.Requests[GPUResourceName]
	if !ok {
		return 0
	}

	// GPU 必须是整数请求
	if gpuQuantity.Value()*1000 != gpuQuantity.MilliValue() {
		return 0
	}

	return int(gpuQuantity.Value())
}

// GetTopologyHints returns topology hints for GPU resources
func (gm *gpuManager) GetTopologyHints(container *v1.Container,
	topoInfo *api.NumatopoInfo, resNumaSets api.ResNumaSets) map[string][]policy.TopologyHint {

	// 1. 检查是否请求 GPU
	requestNum := guaranteedGPUs(container)
	if requestNum == 0 {
		klog.V(4).Infof("Container %s does not request GPUs", container.Name)
		return nil
	}

	// 2. 检查节点是否有 GPU 拓扑信息
	if topoInfo.GPUDetail == nil || len(topoInfo.GPUDetail) == 0 {
		klog.V(4).Infof("Node has no GPU topology info")
		return nil
	}

	// 3. 获取可用的 GPU 集合
	availableGPUs, ok := resNumaSets[GPUResourceName]
	if !ok || availableGPUs.IsEmpty() {
		klog.Warningf("No available GPUs on node")
		return nil
	}

	// 4. 获取 NUMA 节点列表
	numaNodes := topoInfo.CPUDetail.NUMANodes().List()
	if len(numaNodes) == 0 {
		klog.Warningf("No NUMA nodes found on node")
		return nil
	}

	// 5. 生成拓扑提示
	hints := generateGPUTopologyHints(requestNum, availableGPUs, topoInfo.GPUDetail, numaNodes)
	klog.V(4).Infof("Generated %d GPU topology hints for container %s", len(hints), container.Name)

	return map[string][]policy.TopologyHint{
		GPUResourceName: hints,
	}
}

// generateGPUTopologyHints generates topology hints for GPU allocation
func generateGPUTopologyHints(
	request int,
	availableGPUs cpuset.CPUSet,
	gpuDetail map[string]nodeinfov1alpha1.GPUInfo,
	numaNodes []int,
) []policy.TopologyHint {

	// 构建 GPU -> NUMA 映射
	gpu2NUMA := make(map[int]int)
	for gpuIdx, gpuInfo := range gpuDetail {
		idx, err := strconv.Atoi(gpuIdx)
		if err != nil {
			klog.Warningf("Invalid GPU index: %s", gpuIdx)
			continue
		}
		gpu2NUMA[idx] = gpuInfo.NUMANodeID
	}

	hints := []policy.TopologyHint{}
	minAffinitySize := len(numaNodes)

	// 使用 bitmask 遍历所有可能的 NUMA 节点组合
	bitmask.IterateBitMasks(numaNodes, func(mask bitmask.BitMask) {
		numaSet := mask.GetBits()

		// 计算该 NUMA 组合中有多少可用的 GPU
		gpuCount := 0
		for _, gpuIdx := range availableGPUs.List() {
			if numaID, ok := gpu2NUMA[gpuIdx]; ok {
				if contains(numaSet, numaID) {
					gpuCount++
				}
			}
		}

		if gpuCount < request {
			return
		}

		// 更新最小 NUMA 节点数
		if mask.Count() < minAffinitySize {
			minAffinitySize = mask.Count()
		}

		// 创建提示
		hint := policy.TopologyHint{
			NUMANodeAffinity: mask,
			Preferred:        false, // 稍后标记首选
		}

		hints = append(hints, hint)
	})

	// 标记最小 NUMA 节点数的提示为首选
	for i := range hints {
		if hints[i].NUMANodeAffinity.Count() == minAffinitySize {
			hints[i].Preferred = true
		}
	}

	return hints
}

// Allocate allocates GPU resources based on topology hints
func (gm *gpuManager) Allocate(container *v1.Container, bestHit *policy.TopologyHint,
	topoInfo *api.NumatopoInfo, resNumaSets api.ResNumaSets) map[string]cpuset.CPUSet {

	// 1. 获取请求的 GPU 数量
	requestNum := guaranteedGPUs(container)
	if requestNum == 0 {
		return nil
	}

	// 2. 获取可用的 GPU 集合
	availableGPUs, ok := resNumaSets[GPUResourceName]
	if !ok || availableGPUs.IsEmpty() {
		klog.Errorf("No available GPUs for allocation")
		return nil
	}

	// 3. 如果没有拓扑提示，直接分配前 N 个可用 GPU
	if bestHit == nil || bestHit.NUMANodeAffinity == nil {
		allocatedGPUs := cpuset.New()
		for _, gpuIdx := range availableGPUs.List() {
			allocatedGPUs = allocatedGPUs.Union(cpuset.New(gpuIdx))
			if allocatedGPUs.Size() >= requestNum {
				break
			}
		}

		return map[string]cpuset.CPUSet{
			GPUResourceName: allocatedGPUs,
		}
	}

	// 4. 根据 TopologyHint 筛选 GPU
	numaNodes := bestHit.NUMANodeAffinity.GetBits()

	// 构建 GPU -> NUMA 映射
	gpu2NUMA := make(map[int]int)
	for gpuIdx, gpuInfo := range topoInfo.GPUDetail {
		idx, _ := strconv.Atoi(gpuIdx)
		gpu2NUMA[idx] = gpuInfo.NUMANodeID
	}

	// 选择在目标 NUMA 节点上的 GPU
	selectedGPUs := cpuset.New()
	for _, gpuIdx := range availableGPUs.List() {
		if numaID, ok := gpu2NUMA[gpuIdx]; ok {
			if contains(numaNodes, numaID) {
				selectedGPUs = selectedGPUs.Union(cpuset.New(gpuIdx))

				if selectedGPUs.Size() >= requestNum {
					break
				}
			}
		}
	}

	if selectedGPUs.Size() < requestNum {
		// If we couldn't allocate enough GPUs from the preferred NUMA nodes,
		// try to allocate remaining GPUs from other available NUMA nodes.
		// This provides graceful degradation for cross-NUMA GPU allocations.
		remainingNeeded := requestNum - selectedGPUs.Size()
		klog.Warningf("Only allocated %d/%d GPUs from preferred NUMA nodes, trying to allocate remaining %d from other NUMA nodes",
			selectedGPUs.Size(), requestNum, remainingNeeded)

		for _, gpuIdx := range availableGPUs.List() {
			if selectedGPUs.Contains(gpuIdx) {
				continue // Already selected
			}
			selectedGPUs = selectedGPUs.Union(cpuset.New(gpuIdx))

			if selectedGPUs.Size() >= requestNum {
				break
			}
		}
	}

	if selectedGPUs.Size() < requestNum {
		klog.Errorf("Failed to allocate %d GPUs, only %d available in total",
			requestNum, selectedGPUs.Size())
		return map[string]cpuset.CPUSet{
			GPUResourceName: cpuset.New(),
		}
	}

	return map[string]cpuset.CPUSet{
		GPUResourceName: selectedGPUs,
	}
}

// contains checks if slice contains item
func contains(slice []int, item int) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}
