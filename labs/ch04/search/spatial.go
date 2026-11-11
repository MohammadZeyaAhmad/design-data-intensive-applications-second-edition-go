package search

import (
	"math"
	"sort"
)

type Box struct{ MinLon, MinLat, MaxLon, MaxLat float64 }

func (b Box) valid() bool {
	return (Point{b.MinLon, b.MinLat}).valid() && (Point{b.MaxLon, b.MaxLat}).valid() && b.MinLat <= b.MaxLat
}

type geoDoc struct {
	id      string
	version uint64
	p       Point
}
type kdNode struct {
	box         Box
	left, right *kdNode
	points      []geoDoc
}
type SpatialResult struct {
	Hits []Hit `json:"hits"`
	Work Work  `json:"work"`
}

func kdBuild(points []geoDoc, depth int) *kdNode {
	if len(points) == 0 {
		return nil
	}
	n := &kdNode{box: Box{points[0].p.Lon, points[0].p.Lat, points[0].p.Lon, points[0].p.Lat}}
	for _, p := range points {
		n.box.MinLon = math.Min(n.box.MinLon, p.p.Lon)
		n.box.MaxLon = math.Max(n.box.MaxLon, p.p.Lon)
		n.box.MinLat = math.Min(n.box.MinLat, p.p.Lat)
		n.box.MaxLat = math.Max(n.box.MaxLat, p.p.Lat)
	}
	if len(points) <= 8 {
		n.points = append([]geoDoc(nil), points...)
		return n
	}
	axis := depth % 2
	sort.Slice(points, func(i, j int) bool {
		a, b := points[i], points[j]
		x, y := a.p.Lon, b.p.Lon
		if axis == 1 {
			x, y = a.p.Lat, b.p.Lat
		}
		if x != y {
			return x < y
		}
		return a.id < b.id
	})
	mid := len(points) / 2
	n.left = kdBuild(points[:mid], depth+1)
	n.right = kdBuild(points[mid:], depth+1)
	return n
}
func contains(b Box, p Point) bool {
	return p.Lat >= b.MinLat && p.Lat <= b.MaxLat && (b.MinLon <= b.MaxLon && p.Lon >= b.MinLon && p.Lon <= b.MaxLon || b.MinLon > b.MaxLon && (p.Lon >= b.MinLon || p.Lon <= b.MaxLon))
}
func overlaps(a, b Box) bool {
	return a.MinLon <= b.MaxLon && a.MaxLon >= b.MinLon && a.MinLat <= b.MaxLat && a.MaxLat >= b.MinLat
}
func (n *kdNode) rangeQuery(b Box, out *[]geoDoc, work *Work) {
	if n == nil {
		return
	}
	work.Nodes++
	if !overlaps(n.box, b) {
		return
	}
	if n.points != nil {
		for _, p := range n.points {
			work.Candidates++
			if contains(b, p.p) {
				*out = append(*out, p)
			}
		}
		return
	}
	n.left.rangeQuery(b, out, work)
	n.right.rangeQuery(b, out, work)
}
func splitBox(b Box) []Box {
	if b.MinLon <= b.MaxLon {
		return []Box{b}
	}
	return []Box{{b.MinLon, b.MinLat, 180, b.MaxLat}, {-180, b.MinLat, b.MaxLon, b.MaxLat}}
}
func (n *kdNode) boxQuery(b Box) ([]geoDoc, Work) {
	var points []geoDoc
	var work Work
	seen := map[string]bool{}
	for _, part := range splitBox(b) {
		var hits []geoDoc
		n.rangeQuery(part, &hits, &work)
		for _, p := range hits {
			if !seen[p.id] {
				seen[p.id] = true
				points = append(points, p)
			}
		}
	}
	return points, work
}

const earthKM = 6371.0088

func Haversine(a, b Point) float64 {
	rad := math.Pi / 180
	dlat, dlon := (b.Lat-a.Lat)*rad, (b.Lon-a.Lon)*rad
	x := math.Sin(dlat/2)*math.Sin(dlat/2) + math.Cos(a.Lat*rad)*math.Cos(b.Lat*rad)*math.Sin(dlon/2)*math.Sin(dlon/2)
	return 2 * earthKM * math.Asin(math.Sqrt(math.Max(0, math.Min(1, x))))
}
func longitude(x float64) float64 { return math.Mod(x+540, 360) - 180 }
func radiusBox(p Point, km float64) Box {
	r := km / earthKM
	deg := 180 / math.Pi
	latMin, latMax := p.Lat-r*deg-1e-9, p.Lat+r*deg+1e-9
	if latMin <= -90 || latMax >= 90 || r >= math.Pi/2 {
		return Box{-180, math.Max(-90, latMin), 180, math.Min(90, latMax)}
	}
	ratio := math.Sin(r) / math.Cos(p.Lat/deg)
	if ratio >= 1 {
		return Box{-180, latMin, 180, latMax}
	}
	delta := math.Asin(ratio)*deg + 1e-9
	return Box{longitude(p.Lon - delta), latMin, longitude(p.Lon + delta), latMax}
}
