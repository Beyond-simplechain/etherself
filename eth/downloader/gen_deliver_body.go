package downloader

import (
	"fmt"
	"github.com/ethereum/go-ethereum/core/types"
)

func (q *queue) deliver_body(id string, txLists [][]*types.Transaction, uncleLists [][]*types.Header) (int, error) {

	// Short circuit if the data was never requested
	//:确保像这个节点请求过body
	request := q.blockPendPool[id]
	if request == nil {
		return 0, errNoFetchesPending
	}
	bodyReqTimer.UpdateSince(request.Time)
	delete(q.blockPendPool, id)

	// If no data items were retrieved, mark them as unavailable for the origin peer
	if len(txLists) == 0 {
		for _, header := range request.Headers {
			request.Peer.MarkLacking(header.Hash())
		}
	}
	// Assemble each of the results with their headers and retrieved data parts
	var (
		accepted int
		failure  error
		useful   bool
	)
	for i, header := range request.Headers {
		// Short circuit assembly if no more fetch results are found
		if i >= len(txLists) {
			break
		}
		// Reconstruct the next result if contents match up
		index := int(header.Number.Int64() - int64(q.resultOffset))
		if index >= len(q.resultCache) || index < 0 || q.resultCache[index] == nil {
			failure = errInvalidChain
			break
		}
		if types.DeriveSha(types.Transactions(txLists[i])) != header.TxHash || types.CalcUncleHash(uncleLists[i]) != header.UncleHash {
			failure = errInvalidBody
			break
		}
		q.resultCache[index].Transactions = txLists[i]
		q.resultCache[index].Uncles = uncleLists[i]
		hash := header.Hash()

		q.blockDonePool[hash] = struct{}{}
		q.resultCache[index].Pending--
		useful = true
		accepted++

		// Clean up a successful fetch
		//:成功设置为nil
		request.Headers[i] = nil
		delete(q.blockTaskPool, hash)
	}
	// Return all failed or missing fetches to the queue
	//:失败重新加入fetch队列
	for _, header := range request.Headers {
		if header != nil {
			q.blockTaskQueue.Push(header, -int64(header.Number.Uint64()))
		}
	}
	// Wake up Results
	//:唤醒active信号，使得queue.Results()执行
	if accepted > 0 {
		q.active.Signal()
	}
	// If none of the data was good, it's a stale delivery
	switch {
	case failure == nil || failure == errInvalidChain:
		return accepted, failure
	case useful:
		return accepted, fmt.Errorf("partial failure: %v", failure)
	default:
		return accepted, errStaleDelivery
	}
}
