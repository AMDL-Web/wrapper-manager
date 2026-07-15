package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"golang.org/x/sync/singleflight"
	"io"
	"math/rand"
	"net/http"
	"sync"
	"time"
)

const regionRequestTimeout = 15 * time.Second

var (
	SongRegionCache        sync.Map
	songRegionSingleFlight singleflight.Group
)

func checkAvailableOnRegion(adamId string, region string, mv bool) (bool, error) {
	return checkAvailableOnRegionContext(context.Background(), adamId, region, mv)
}

func checkAvailableOnRegionContext(ctx context.Context, adamId string, region string, mv bool) (bool, error) {
	var cacheKey string
	if mv {
		cacheKey = fmt.Sprintf("mv/%s/%s", region, adamId)
	} else {
		cacheKey = fmt.Sprintf("song/%s/%s", region, adamId)
	}
	if result, ok := SongRegionCache.Load(cacheKey); ok {
		return result.(bool), nil
	}

	resultCh := songRegionSingleFlight.DoChan(cacheKey, func() (interface{}, error) {
		if adamId == "0" {
			return true, nil
		}

		var url string
		if mv {
			url = fmt.Sprintf("https://amp-api.music.apple.com/v1/catalog/%s/music-videos/%s", region, adamId)
		} else {
			url = fmt.Sprintf("https://amp-api.music.apple.com/v1/catalog/%s/songs/%s", region, adamId)
		}
		token, err := GetToken()
		if err != nil {
			return false, err
		}
		// The shared request must not inherit the first caller's cancellation:
		// other streams for the same song may still be waiting on this flight.
		requestCtx, cancel := context.WithTimeout(context.Background(), regionRequestTimeout)
		defer cancel()
		req, err := http.NewRequestWithContext(requestCtx, "GET", url, nil)
		if err != nil {
			return false, err
		}
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
		req.Header.Set("User-Agent", "Mozilla/5.0 ...")
		req.Header.Set("Origin", "https://music.apple.com")

		resp, err := GetHttpClient().Do(req)
		if err != nil {
			return false, err
		}
		defer resp.Body.Close()

		respBody, err := io.ReadAll(resp.Body)
		if err != nil {
			return false, err
		}
		var respJson map[string][]interface{}
		if err := json.Unmarshal(respBody, &respJson); err != nil {
			return false, err
		}

		if respJson["errors"] != nil {
			return false, nil
		}

		available := respJson["data"] != nil
		SongRegionCache.Store(cacheKey, available)
		return available, nil
	})

	var val interface{}
	var err error
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case result := <-resultCh:
		val, err = result.Val, result.Err
	}

	if err != nil {
		return false, err
	}
	available, ok := val.(bool)
	if !ok {
		return false, errors.New("unexpected region availability result")
	}
	return available, nil
}

func SelectInstance(adamId string) (string, error) {
	var selectedInstances []string
	for _, instance := range Instances {
		available, err := checkAvailableOnRegion(adamId, instance.Region, false)
		if err != nil {
			return "", err
		}
		if available {
			selectedInstances = append(selectedInstances, instance.Id)
		}
	}
	if len(selectedInstances) == 0 {
		for _, instance := range Instances {
			available, err := checkAvailableOnRegion(adamId, instance.Region, true)
			if err != nil {
				return "", err
			}
			if available {
				selectedInstances = append(selectedInstances, instance.Id)
			}
		}
	}
	if len(selectedInstances) != 0 {
		return selectedInstances[rand.Intn(len(selectedInstances))], nil
	}
	return "", nil
}

func SelectInstanceForLyrics(adamId string, language string) string {
	token, err := GetToken()
	if err != nil {
		return ""
	}
	var selectedInstances []string
	for _, instance := range Instances {
		musicToken, err := GetMusicToken(instance)
		if err != nil {
			return ""
		}
		if HasLyrics(adamId, instance.Region, language, token, musicToken) {
			selectedInstances = append(selectedInstances, instance.Id)
		}
	}
	if len(selectedInstances) != 0 {
		return selectedInstances[rand.Intn(len(selectedInstances))]
	}
	return ""
}
