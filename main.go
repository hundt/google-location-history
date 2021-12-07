package main

import (
	"encoding/gob"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/MadAppGang/kdbush"
	"github.com/asmarques/geodist"
)

type Location struct {
	Latitude  int64  `json:"latitudeE7"`
	Longitude int64  `json:"longitudeE7"`
	Timestamp string `json:"timestampMs"`
}

type LocationHistory struct {
	Pinpoints []*Location `json:"locations"`
}

type ConvertedLocation struct {
	Latitude  float64
	Longitude float64
	Time      time.Time
}

func (c ConvertedLocation) Coordinates() (X, Y float64) {
	return c.Latitude, c.Longitude
}

var debug = flag.Bool("debug", false, "show debug logging")
var latitude = flag.Float64("lat", 36.461755, "latitude of target location")
var longitude = flag.Float64("long", -116.866612, "longitude of target location")
var cacheData = flag.Bool("cache-data", true, "enable caching of a more easily processed form of the data")
var address = flag.String("address", "", "address to look up instead of specifying lat/long (requires -google-api-key)")
var googleApiKey = flag.String("google-api-key", "", "API Key for Google Geocoding API, for use with -address")
var threshold = flag.String("threshold", "50m", "threshold used to determine whether you are at the location")

func getLocation(address string) (*geodist.Point, error) {
	geocodeURL := fmt.Sprintf(
		"https://maps.googleapis.com/maps/api/geocode/json?key=%s&address=%s",
		*googleApiKey,
		url.QueryEscape(address))
	response, err := http.Get(geocodeURL)
	if err != nil {
		return nil, fmt.Errorf("Error fetching Google geocode results: %s", err)
	}
	if response.StatusCode != 200 {
		body, err := ioutil.ReadAll(response.Body)
		if err != nil {
			log.Printf("Error reading error response body: %s", err)
		}
		return nil, fmt.Errorf("Got code %d from Geocoding API. Response: %s", response.StatusCode, body)
	}
	type GoogleLocation struct {
		Latitude  float64 `json:"lat"`
		Longitude float64 `json:"lng"`
	}
	type GoogleGeometry struct {
		Location GoogleLocation `json:"location"`
	}
	type GoogleResult struct {
		Address  string         `json:"formatted_address"`
		Geometry GoogleGeometry `json:"geometry"`
	}
	type GoogleResponse struct {
		Results []GoogleResult `json:"results"`
		Status  string         `json:"status"`
		Error   string         `json:"error_message"`
	}
	gr := &GoogleResponse{}
	err = json.NewDecoder(response.Body).Decode(&gr)
	if err != nil {
		return nil, fmt.Errorf("Error decoding response from Google Geocoding API: %s", err)
	}
	if gr.Status != "OK" {
		return nil, fmt.Errorf("Error from Google Geocoding API: %s", gr.Error)
	}
	if len(gr.Results) == 0 {
		return nil, fmt.Errorf("No results from Google Geocoding API for %q", address)
	}
	log.Printf("Resolved to full address %q", gr.Results[0].Address)
	return &geodist.Point{
		Lat:  gr.Results[0].Geometry.Location.Latitude,
		Long: gr.Results[0].Geometry.Location.Longitude,
	}, nil
}

func parseDistance(dist string) (km float64, err error) {
	units := []struct {
		abbrev string
		perKM  float64
	}{
		{
			"km", 1,
		},
		{
			"ft", 3280.84,
		},
		{
			"mi", 0.621371,
		},
		{
			"m", 1000,
		},
	}
	dist = strings.ToLower(strings.TrimSpace(dist))
	for _, unit := range units {
		if strings.HasSuffix(dist, unit.abbrev) {
			dist = strings.TrimSpace(strings.TrimSuffix(dist, unit.abbrev))
			count, err := strconv.ParseFloat(dist, 64)
			if err != nil {
				return 0, fmt.Errorf("Error parsing distance %q: %s", dist, err)
			}
			distKM := count / unit.perKM
			log.Printf("Using distance %.3fkm", distKM)
			return distKM, nil
		}
	}
	return 0, fmt.Errorf("No recognized units in distance %q", dist)
}

type direction float64

const (
	north direction = 1
	east  direction = 1
	south direction = -1
	west  direction = -1
)

func (d direction) reverse() direction {
	return direction(d * -1)
}

func find(p1 geodist.Point, p2 *geodist.Point, dir direction, adjust *float64, limit float64, targetDistance float64) error {
	inc := 1e-6
	for {
		*adjust += inc * float64(dir)
		if *adjust*float64(dir) > limit*float64(dir) {
			return fmt.Errorf("too close to a pole or meridian")
		}
		d, err := geodist.VincentyDistance(p1, *p2)
		if err != nil {
			return fmt.Errorf("Error computing distance: %s", err)
		}
		if d > targetDistance {
			break
		}
		inc *= 2
	}
	// Now p2 is > limit away from p1. Take p3 (starting with p1) and p2 as two points on either
	// side of the d=limit line. Take the midpoint M between them and throw away either p3 or p2
	// so that what's left and M are on either side of the line. Repeat until distance is < 10cm
	threshold := 0.0001
	a := p1
	b := *p2
	for {
		m := geodist.Point{
			Lat:  (a.Lat + b.Lat) / 2,
			Long: (a.Long + b.Long) / 2,
		}
		d, err := geodist.VincentyDistance(p1, m)
		if err != nil {
			return fmt.Errorf("Error computing distance (phase 2): %s", err)
		}
		delta := d - targetDistance
		if delta < 0 {
			delta = -delta
		}
		if delta < threshold {
			*p2 = m
			return nil
		}
		if d > targetDistance {
			b = m
		} else {
			a = m
		}
	}
}

func findBoundingBox(p geodist.Point, size float64) (ne, sw *geodist.Point, err error) {
	e := p
	err = find(p, &e, east, &e.Long, 180, size)
	if err != nil {
		return nil, nil, err
	}
	n := p
	err = find(p, &n, north, &n.Lat, 90, size)
	if err != nil {
		return nil, nil, err
	}
	w := p
	err = find(p, &w, west, &w.Long, -180, size)
	if err != nil {
		return nil, nil, err
	}
	s := p
	err = find(p, &s, south, &s.Lat, -90, size)
	if err != nil {
		return nil, nil, err
	}
	ne = &geodist.Point{
		Lat:  n.Lat,
		Long: e.Long,
	}
	sw = &geodist.Point{
		Lat:  s.Lat,
		Long: w.Long,
	}
	return ne, sw, nil
}

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s /path/to/Location\\ History.json\n", os.Args[0])
		flag.PrintDefaults()
	}

	flag.Parse()

	args := flag.Args()
	if len(args) != 1 {
		flag.Usage()
		os.Exit(2)
	}

	takeoutFile := args[0]
	target := &geodist.Point{
		Lat:  *latitude,
		Long: *longitude,
	}

	if *address != "" {
		var err error
		target, err = getLocation(*address)
		if err != nil {
			log.Fatalf("Error geocoding address: %s", err)
		}
	}

	log.Printf("Using target (%.6f, %.6f)", target.Lat, target.Long)

	threshholdKm, err := parseDistance(*threshold)
	if err != nil {
		log.Fatalf("%s", err)
	}

	ne, sw, err := findBoundingBox(*target, threshholdKm*2)
	if err != nil {
		log.Fatalf("Error finding bounding box: %s", err)
	}
	if *debug {
		log.Printf("Bounding box: %v %v", ne, sw)
	}

	minPinpointCount := 10
	// TODO: deal with pinpoints too far apart?

	usedCache := false

	converted := []ConvertedLocation{}

	if *cacheData {
		f, err := os.Open(takeoutFile + ".dat")
		if os.IsNotExist(err) {
			// That's fine, we will create it later.
		} else if err != nil {
			log.Fatalf("Error opening cache file: %s", err)
		} else {
			err = gob.NewDecoder(f).Decode(&converted)
			if err != nil {
				log.Fatalf("Error loading cache file: %s", err)
			}
			f.Close()
			usedCache = true
		}
	}
	if !usedCache {
		history := &LocationHistory{}
		f, err := os.Open(takeoutFile)
		if err != nil {
			log.Fatalf("Error opening takeout file: %s", err)
		}
		err = json.NewDecoder(f).Decode(history)
		if err != nil {
			log.Fatalf("Error loading takeout file: %s", err)
		}
		f.Close()

		converted = make([]ConvertedLocation, len(history.Pinpoints))
		for idx, location := range history.Pinpoints {
			secondsString := location.Timestamp[:len(location.Timestamp)-3]
			seconds, err := strconv.ParseInt(secondsString, 10, 64)
			if err != nil {
				log.Fatalf("Error parsing time %q", secondsString) // TODO: not fatal
				continue
			}
			converted[idx] = ConvertedLocation{
				Time:      time.Unix(seconds, 0),
				Latitude:  float64(location.Latitude) / 1e7,
				Longitude: float64(location.Longitude) / 1e7,
			}
		}
	}

	if *cacheData && !usedCache && len(converted) > 0 {
		f, err := os.Create(takeoutFile + ".dat")
		if err != nil {
			log.Fatalf("Error opening cache file for write: %s", err)
		}
		err = gob.NewEncoder(f).Encode(converted)
		if err != nil {
			log.Fatalf("Error writing cache file: %s", err)
		}
		f.Close()
	}

	points := make([]kdbush.Point, len(converted))
	for idx, location := range converted {
		points[idx] = location
	}
	log.Printf("Loaded %d pinpoints", len(points))

	bush := kdbush.NewBush(points, 64)
	indexes := bush.Range(sw.Lat, sw.Long, ne.Lat, ne.Long)
	if len(indexes) == 0 {
		return
	}
	candidates := map[int]struct{}{}
	for _, idx := range indexes {
		candidates[idx] = struct{}{}
	}
	sort.Ints(indexes)
	var startTime, endTime time.Time
	totalInsideCount, consecutiveInsideCount, maxConsecutiveInsideCount, consecutiveOutsideCount := 0, 0, 0, 0
	p := geodist.Point{}
	for idx := indexes[0]; idx <= indexes[len(indexes)-1]; idx++ {
		loc := points[idx].(ConvertedLocation)
		d := threshholdKm * 2
		if _, isCandidate := candidates[idx]; isCandidate {
			p.Lat = loc.Latitude
			p.Long = loc.Longitude
			d, err = geodist.VincentyDistance(p, *target)
			if err != nil {
				if *debug {
					log.Printf("Skipping %v: %s", loc, err)
				}
				continue
			}
		}
		if d < threshholdKm {
			if *debug {
				log.Printf("Distance %.0fm at %s", d*1000, loc.Time)
			}
			if totalInsideCount == 0 {
				startTime = loc.Time
			}
			endTime = loc.Time
			totalInsideCount++
			consecutiveInsideCount++
			if consecutiveInsideCount > maxConsecutiveInsideCount {
				maxConsecutiveInsideCount = consecutiveInsideCount
			}
			consecutiveOutsideCount = 0
		} else {
			consecutiveOutsideCount++
			consecutiveInsideCount = 0
		}
		if consecutiveOutsideCount >= minPinpointCount || idx == indexes[len(indexes)-1] {
			if maxConsecutiveInsideCount >= minPinpointCount {
				log.Printf("Visited for %s starting at %s (%d pinpoints / %d max consecutive)", endTime.Sub(startTime), startTime, totalInsideCount, maxConsecutiveInsideCount)
			} else if totalInsideCount > 0 {
				if *debug {
					log.Printf("Dropped visit for %s starting at %s (%d pinpoints / %d max consecutive)", endTime.Sub(startTime), startTime, totalInsideCount, maxConsecutiveInsideCount)
				}
			}
			totalInsideCount = 0
			maxConsecutiveInsideCount = 0
		}
	}
}
