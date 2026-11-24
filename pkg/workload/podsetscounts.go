/*
Copyright The Kubernetes Authors.

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

package workload

import (
	"maps"

	"k8s.io/utils/ptr"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/constants"
	"sigs.k8s.io/kueue/pkg/features"
	utilslices "sigs.k8s.io/kueue/pkg/util/slices"
)

// PodSetsCounts holds one replica count per PodSet, such as the requested, granted or
// reclaimable count. Keying by PodSet name lets callers combine counts taken from lists
// that are not in the same order, such as the spec and the admission.
type PodSetsCounts map[kueue.PodSetReference]int32

// HasSamePodSetKeys returns true if both PodSetsCounts have exactly the same PodSet keys,
// ignoring their replica count values.
func (c PodSetsCounts) HasSamePodSetKeys(in PodSetsCounts) bool {
	return maps.EqualFunc(c, in, func(_ int32, _ int32) bool { return true })
}

// EqualTo returns true if this PodSetsCount is identical (on keys and values) to
// the incoming PodSetsCount.
func (c PodSetsCounts) EqualTo(in PodSetsCounts) bool {
	return maps.Equal(c, in)
}

// HasFewerReplicasThan returns true if there is at least one pod set in the current set (`c`)
// whose count is lower than the corresponding entry in the input set (`in`).
//
// Note: This function does not consider keys that exist in current set (`c`) but not the other (`in`).
func (c PodSetsCounts) HasFewerReplicasThan(in PodSetsCounts) bool {
	for key, cCount := range c {
		if inCount, ok := in[key]; ok && cCount < inCount {
			return true
		}
	}
	return false
}

// ExtractPodSetCounts builds a PodSetsCounts map from a list of PodSets.
// Each entry maps PodSet name to its replica count.
func ExtractPodSetCounts(podSets []kueue.PodSet) PodSetsCounts {
	return utilslices.ToMap(podSets, func(i int) (kueue.PodSetReference, int32) {
		return podSets[i].Name, podSets[i].Count
	})
}

// ExtractGrantedPodSetCounts builds a PodSetsCounts map from a list of PodSetAssignments.
// Each entry maps PodSet name to its replica count.
func ExtractGrantedPodSetCounts(wl *kueue.Workload) PodSetsCounts {
	if wl.Status.Admission == nil {
		return nil
	}
	counts := ExtractPodSetCounts(wl.Spec.PodSets)
	granted := make(PodSetsCounts, len(wl.Status.Admission.PodSetAssignments))
	for _, psa := range wl.Status.Admission.PodSetAssignments {
		count := counts[psa.Name]
		granted[psa.Name] = min(ptr.Deref(psa.Count, count), count)
	}
	return granted
}

// ExtractGrantedPodSetCountsAfterReclaim is ExtractGrantedPodSetCounts with
// each granted count additionally capped at the PodSet's count after
// reclaimable pods are subtracted, i.e. the number of pods the Workload still
// effectively runs. It returns nil when the Workload has no admission.
func ExtractGrantedPodSetCountsAfterReclaim(wl *kueue.Workload) PodSetsCounts {
	granted := ExtractGrantedPodSetCounts(wl)
	if granted == nil {
		return nil
	}
	current := podSetsCountsAfterReclaim(wl)
	for name, count := range granted {
		granted[name] = min(count, current[name])
	}
	return granted
}

// ExtractPodSetCountsFromWorkload returns a PodSetsCounts map derived from the provided Workload.
//
// Important: This function assumes the Workload is not nil. It does not perform a nil check,
// and calling it with a nil Workload will result in a panic.
func ExtractPodSetCountsFromWorkload(wl *kueue.Workload) PodSetsCounts {
	return ExtractPodSetCounts(wl.Spec.PodSets)
}

// CanReduceMinCountOnScaleDown reports whether an elastic Workload can lower
// its PodSet minimums when partial scale-up is enabled: when it is admitted,
// or when it is a scale-up slice that is not admitted yet. The initial Workload
// keeps its minimums until it is admitted, so its first admission stays
// all-or-nothing.
func CanReduceMinCountOnScaleDown(wl *kueue.Workload) bool {
	return features.Enabled(features.ElasticJobsViaWorkloadSlicesWithPartialReplicaScaleUp) &&
		IsElasticWorkload(wl) &&
		(IsAdmitted(wl) || wl.Annotations[constants.WorkloadSliceReplacementForAnnotation] != "")
}

// keepsInitialAdmissionFloor reports whether an elastic Workload is the initial
// slice of a partial scale-up job that has not reserved quota yet. Its PodSet
// minimums are the all-or-nothing floor of the first admission, so they follow
// the target count while the Workload waits.
func keepsInitialAdmissionFloor(wl *kueue.Workload) bool {
	return features.Enabled(features.ElasticJobsViaWorkloadSlicesWithPartialReplicaScaleUp) &&
		IsElasticWorkload(wl) &&
		!HasQuotaReservation(wl) &&
		wl.Annotations[constants.WorkloadSliceReplacementForAnnotation] == ""
}

// ApplyPodSetCounts updates a Workload's PodSet counts. For elastic Workloads
// that CanReduceMinCountOnScaleDown, a scale-down below a PodSet's minCount
// also lowers that minCount to the new count.
//
// For the initial slice of a partial scale-up job that has not reserved quota
// yet, a minCount equal to the old count moves with the count in both
// directions, so the first admission stays all-or-nothing for the new target,
// and a minCount above the new count is lowered to it, so the update stays valid.
//
// This function only updates existing PodSets within the Workload. It ignores:
//   - PodSet names present in the counts map but not found in the Workload.
//   - PodSets in the Workload that are not present in the counts map.
//
// Important: This function assumes the Workload is not nil. It does not perform a nil check,
// and calling it with a nil Workload will result in a panic.
func ApplyPodSetCounts(wl *kueue.Workload, counts PodSetsCounts) {
	canReduceMinCount := CanReduceMinCountOnScaleDown(wl)
	keepInitialFloor := keepsInitialAdmissionFloor(wl)
	for i := range wl.Spec.PodSets {
		ps := &wl.Spec.PodSets[i]
		if count, found := counts[ps.Name]; found {
			if ps.MinCount != nil {
				switch {
				case keepInitialFloor && (*ps.MinCount == ps.Count || *ps.MinCount > count):
					ps.MinCount = new(count)
				case canReduceMinCount && count < ps.Count && *ps.MinCount > count:
					ps.MinCount = new(count)
				}
			}
			ps.Count = count
		}
	}
}
