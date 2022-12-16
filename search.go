package es_util

import (
	"context"
	"errors"
	"github.com/olivere/elastic/v7"
)

const (
	searchAfterSize = 1000
	maxHit          = 500000
)

func SearchWithExport(makeService func() *elastic.SearchService, export bool) (*elastic.SearchResult, error) {
	if !export {
		return makeService().Do(context.Background())
	}
	sortValues := make([]interface{}, 0)
	var searchResult *elastic.SearchResult
	for {
		service := makeService().From(0).Size(searchAfterSize).TrackTotalHits(true)
		if len(sortValues) > 0 {
			service.SearchAfter(sortValues...)
		}
		currentResult, err := service.Do(context.Background())
		if err != nil {
			return nil, err
		}
		if currentResult.Hits == nil {
			return nil, errors.New("elastic hits is nil")
		}
		if searchResult == nil {
			searchResult = currentResult
		} else {
			searchResult.Hits.Hits = append(searchResult.Hits.Hits, currentResult.Hits.Hits...)
		}
		if len(currentResult.Hits.Hits) < searchAfterSize || len(searchResult.Hits.Hits) >= maxHit {
			break
		}
		sortValues = currentResult.Hits.Hits[searchAfterSize-1].Sort
	}
	return searchResult, nil
}
