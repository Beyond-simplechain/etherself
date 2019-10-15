package downloader

import (
	"fmt"
	"github.com/ethereum/go-ethereum/log"
	"time"
)

func (d *Downloader) fetchParts_body() error {

	// Create a ticker to detect expired retrieval tasks
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	update := make(chan struct{}, 1)

	// Prepare the queue and fetch block parts until the block header fetcher's done
	finished := false
	//:1、从reserve中领取blockTaskQueue中的header生成request请求
	//:2、使用fetch向peer节点发送请求
	//:3、由ProtocolManager.handleMsg获取请求返回值并调用Downloader.Deliver()存入deliveryCh
	//:4、从deliveryCh获取请求返回值并调用deliver存入queue(fillHeaderSkeleton:headerTaskQueue|fetchBody:blockTaskQueue|fetchReceipt:receiptTaskQueue)
	for {
		select {
		case <-d.cancelCh:
			return errCancelBodyFetch

		//:receive response
		case packet := <-d.bodyCh:
			// If the peer was previously banned and failed to deliver its pack
			// in a reasonable time frame, ignore its message.
			if peer := d.peers.Peer(packet.PeerId()); peer != nil {
				// Deliver the received chunk of data and check chain validity
				pack := packet.(*bodyPack)
				//:body中的tx、uncle交给queue
				accepted, err := d.queue.DeliverBodies(pack.peerID, pack.transactions, pack.uncles)
				if err == errInvalidChain {
					return err
				}
				// Unless a peer delivered something completely else than requested (usually
				// caused by a timed out request which came through in the end), set it to
				// idle. If the delivery's stale, the peer should have already been idled.
				if err != errStaleDelivery {
					//:将此body请求回复节点设置为空闲(已经请求回收body完成)
					peer.SetBodiesIdle(accepted)
				}
				// Issue a log to the user to see what's going on
				switch {
				case err == nil && packet.Items() == 0:
					peer.log.Trace("Requested data not delivered", "type", "bodies")
				case err == nil:
					peer.log.Trace("Delivered new batch of data", "type", "bodies", "count", packet.Stats())
				default:
					peer.log.Trace("Failed to deliver retrieved data", "type", "bodies", "err", err)
				}
			}
			// Blocks assembled, try to update the progress
			select {
			case update <- struct{}{}:
			default:
			}

		//:processHeaders执行完成后唤醒wakeCh，再发送update信号执行reserve和fetch
		case cont := <-d.bodyWakeCh:
			// The header fetcher sent a continuation flag, check if it's done
			if !cont {
				finished = true
			}
			// Headers arrive, try to update the progress
			select {
			case update <- struct{}{}:
			default:
			}

		case <-ticker.C:
			// Sanity check update the progress
			select {
			case update <- struct{}{}:
			default:
			}

		//:send request
		case <-update:
			// Short circuit if we lost all our peers
			if d.peers.Len() == 0 {
				return errNoPeers
			}
			// Check for fetch request timeouts and demote the responsible peers
			for pid, fails := range d.queue.ExpireBodies(d.requestTTL()) {
				if peer := d.peers.Peer(pid); peer != nil {
					// If a lot of retrieval elements expired, we might have overestimated the remote peer or perhaps
					// ourselves. Only reset to minimal throughput but don't drop just yet. If even the minimal times
					// out that sync wise we need to get rid of the peer.
					//
					// The reason the minimum threshold is 2 is because the downloader tries to estimate the bandwidth
					// and latency of a peer separately, which requires pushing the measures capacity a bit and seeing
					// how response times reacts, to it always requests one more than the minimum (i.e. min 2).
					if fails > 2 {
						peer.log.Trace("Data delivery timed out", "type", "bodies")
						peer.SetBodiesIdle(0)

					} else {
						peer.log.Debug("Stalling delivery, dropping", "type", "bodies")
						if d.dropPeer == nil {
							// The dropPeer method is nil when `--copydb` is used for a local copy.
							// Timeouts can occur if e.g. compaction hits at the wrong time, and can be ignored
							peer.log.Warn("Downloader wants to drop peer, but peerdrop-function is not set", "peer", pid)
						} else {
							d.dropPeer(pid)
						}
					}
				}
			}
			// If there's nothing more to fetch, wait or terminate
			if d.queue.PendingBlocks() == 0 {
				if !d.queue.InFlightBlocks() && finished {
					log.Debug("Data fetching completed", "type", "bodies")
					return nil
				}
				break
			}
			// Send a download request to all idle peers, until throttled
			progressed, throttled, running := false, false, d.queue.InFlightBlocks()
			idles, total := d.peers.BodyIdlePeers()

			//:向空闲的节点请求body
			for _, peer := range idles {
				// Short circuit if throttling activated
				if d.queue.ShouldThrottleBlocks() {
					throttled = true
					break
				}
				// Short circuit if there is no more available task.
				if d.queue.PendingBlocks() == 0 {
					break
				}
				// Reserve a chunk of fetches for a peer. A nil can mean either that
				// no more headers are available, or that the peer is known not to
				// have them.
				//:ReserveHeaders、ReserveBodies、ReserveReceipts返回request
				request, progress, err := d.queue.ReserveBodies(peer, peer.BlockCapacity(d.requestRTT()))
				if err != nil {
					return err
				}
				if progress {
					progressed = true
				}
				if request == nil {
					continue
				}
				if request.From > 0 {
					peer.log.Trace("Requesting new batch of data", "type", "bodies", "from", request.From)
				} else {
					peer.log.Trace("Requesting new batch of data", "type", "bodies", "count", len(request.Headers), "from", request.Headers[0].Number)
				}
				// Fetch the chunk and make sure any errors return the hashes to the queue
				if d.bodyFetchHook != nil {
					d.bodyFetchHook(request.Headers)
				}

				//:FetchHeaders、FetchBodies、FetchReceipts发送request请求
				if err := peer.FetchBodies(request); err != nil {
					// Although we could try and make an attempt to fix this, this error really
					// means that we've double allocated a fetch task to a peer. If that is the
					// case, the internal state of the downloader and the queue is very wrong so
					// better hard crash and note the error instead of silently accumulating into
					// a much bigger issue.
					panic(fmt.Sprintf("%v: %s fetch assignment failed", peer, "bodies"))
				}
				running = true
			}
			// Make sure that we have peers available for fetching. If all peers have been tried
			// and all failed throw an error
			if !progressed && !throttled && !running && len(idles) == total && d.queue.PendingBlocks() > 0 {
				return errPeersUnavailable
			}
		}
	}
}
