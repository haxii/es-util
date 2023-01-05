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
	for i := 0; i < maxHit/searchAfterSize; i++ {
		service := makeService().From(0).Size(searchAfterSize)
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
		if len(currentResult.Hits.Hits) < searchAfterSize {
			break
		}
		last := currentResult.Hits.Hits[searchAfterSize-1]
		if len(last.Sort) == 0 {
			return nil, errors.New("export without sort")
		}
		sortValues = last.Sort
	}
	return searchResult, nil
}
