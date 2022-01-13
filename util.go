package es_util

import (
	"context"
	"github.com/olivere/elastic/v7"
	"github.com/pkg/errors"
	"go.uber.org/multierr"
	"time"
)

func StrArrToInterfaceArray(a []string) []interface{} {
	b := make([]interface{}, len(a))
	for i := range a {
		b[i] = a[i]
	}
	return b
}

func Int64ArrToInterfaceArray(a []int64) []interface{} {
	b := make([]interface{}, len(a))
	for i := range a {
		b[i] = a[i]
	}
	return b
}

type BulkOpt struct {
	Limit          int           // 每批数据量 默认 1000
	Gap            time.Duration // 每批插入的时间间隔 默认 1s
	Refresh        string        // 是否 refresh 默认 false
	IgnoreConflict bool          // 是否忽略冲突 默认 false
}

type SingleReqMaker func(int) elastic.BulkableRequest

// MakeRequest 分批执行 ES 的批量请求，避免 payload 过大造成集群宕机，每批 1k, 间隔 1s
func MakeRequest(es *elastic.Client, bulkSize int, makeSingleRequest SingleReqMaker) (successCount int, err error) {
	return MakeRequestWithOpt(es, bulkSize, BulkOpt{}, makeSingleRequest)
}

// MakeBulkESRequest 分批执行 ES 的批量请求，避免 payload 过大造成集群宕机
//
// bulkSize 最终插入的总数据量, limit 为每批批量插入的数据量, reqGap 为每批插入的时间间隔,
// makeSingleRequest 应按数据的序号返回对应的每个批量请求的具体内容,
// 返回成功的数量
func MakeBulkESRequest(es *elastic.Client, bulkSize, limit int,
	reqGap time.Duration, makeSingleRequest SingleReqMaker) (int, error) {
	return MakeRequestWithOpt(es, bulkSize, BulkOpt{Limit: limit, Gap: reqGap}, makeSingleRequest)
}

// MakeBulkESRequestIgnoreConflict 分批执行 ES 的批量请求，忽略版本冲突, 避免 payload 过大造成集群宕机
//
// bulkSize 最终插入的总数据量, limit 为每批批量插入的数据量, reqGap 为每批插入的时间间隔,
// makeSingleRequest 应按数据的序号返回对应的每个批量请求的具体内容,
// 返回成功的数量
func MakeBulkESRequestIgnoreConflict(es *elastic.Client, bulkSize, limit int,
	reqGap time.Duration, makeSingleRequest SingleReqMaker) (int, error) {
	return MakeRequestWithOpt(es, bulkSize, BulkOpt{Limit: limit, Gap: reqGap, IgnoreConflict: true}, makeSingleRequest)
}

func MakeRequestWithOpt(es *elastic.Client, bulkSize int, opt BulkOpt,
	makeSingleRequest SingleReqMaker) (successCount int, err error) {
	// 保证传入数据的正确性
	limit := opt.Limit
	ignoreConflict := opt.IgnoreConflict
	refresh := opt.Refresh
	reqGap := opt.Gap
	if es == nil || makeSingleRequest == nil {
		return 0, errors.New("invalid es client or index")
	}
	if bulkSize <= 0 {
		return 0, nil
	}
	if limit <= 0 {
		limit = 1000
	}
	if limit > bulkSize {
		limit = bulkSize
	}
	if len(refresh) == 0 {
		refresh = "false"
	}
	if reqGap <= 0 {
		reqGap = time.Second
	}

	// 分割次数
	round := bulkSize/limit + 1
	shouldSleep := false
	for roundIndex := 0; roundIndex < round; roundIndex++ {
		bulkRequest := es.Bulk()
		bulkRequestValid := false // 判断是否真的有请求加到里面去了
		// 每轮执行 bulk request
		for t := 0; t < limit; t++ {
			requestIndex := roundIndex*limit + t
			if requestIndex >= bulkSize {
				break
			}
			req := makeSingleRequest(requestIndex)
			if req != nil {
				bulkRequest.Add(req)
				bulkRequestValid = true
			}
		}
		if !bulkRequestValid {
			continue
		}
		if shouldSleep { // 非第一次批量请求前执行 sleep
			time.Sleep(reqGap)
		}
		resp, bulkReqErr := bulkRequest.Refresh(refresh).Do(context.TODO())
		shouldSleep = true
		if bulkReqErr != nil {
			return 0, bulkReqErr
		}
		roundCount, roundErr := parseBulkResponse(resp, ignoreConflict)
		if roundErr != nil {
			err = multierr.Append(err, roundErr)
		}
		successCount += roundCount
	}
	return
}

func ParseBulkResponseIgnoreConflict(resp *elastic.BulkResponse) (int, error) {
	return parseBulkResponse(resp, true)
}

func ParseBulkResponse(resp *elastic.BulkResponse) (int, error) {
	return parseBulkResponse(resp, false)
}

func parseBulkResponse(resp *elastic.BulkResponse, ignoreConflict bool) (successCount int, err error) {
	if len(resp.Failed()) > 0 {
		for _, f := range resp.Failed() {
			if f.Error == nil {
				continue
			}
			if ignoreConflict && f.Error.Type == "version_conflict_engine_exception" {
				// 不关心重复插入数据的错误
				continue
			}
			err = multierr.Append(err, errors.Errorf("es error: %s [type=%s]", f.Error.Reason, f.Error.Type))
		}
		successCount = len(resp.Items) - len(resp.Failed())
	} else {
		successCount = len(resp.Succeeded())
	}
	return
}
