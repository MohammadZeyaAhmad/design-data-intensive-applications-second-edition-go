package singleleader

// FailureDomain models only placement correlation, not cloud availability,
// quorum, automatic failover, independent storage or WAN latency.
type FailureDomain struct{ Region, Zone string }
type DomainEvidence struct {
	Placement              string `json:"placement"`
	FailedRegion           string `json:"failed_region,omitempty"`
	FailedZone             string `json:"failed_zone,omitempty"`
	SurvivingDurableCopies int    `json:"surviving_durable_copies"`
}

func survivingCopies(nodes []FailureDomain, region, zone string) int {
	n := 0
	for _, node := range nodes {
		if node.Region == region && (zone == "" || node.Zone == zone) {
			continue
		}
		n++
	}
	return n
}
func domainExamples() []DomainEvidence {
	same := []FailureDomain{{"region-a", "zone-1"}, {"region-a", "zone-1"}}
	split := []FailureDomain{{"region-a", "zone-1"}, {"region-a", "zone-2"}}
	regions := []FailureDomain{{"region-a", "zone-1"}, {"region-b", "zone-1"}}
	return []DomainEvidence{
		{"two synchronous copies in one zone", "region-a", "zone-1", survivingCopies(same, "region-a", "zone-1")},
		{"two synchronous copies in separate zones", "region-a", "zone-1", survivingCopies(split, "region-a", "zone-1")},
		{"two synchronous copies in separate zones", "region-a", "", survivingCopies(split, "region-a", "")},
		{"two synchronous copies in separate regions", "region-a", "", survivingCopies(regions, "region-a", "")},
	}
}
