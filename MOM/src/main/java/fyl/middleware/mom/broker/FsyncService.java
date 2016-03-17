package fyl.middleware.mom.broker;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import fyl.middleware.mom.data.DataHelper;

/**
 * 刷盘专用线程
 * 为了确保netty worker不被阻塞（最大化IO吞吐量），刷盘采取了单独线程、多路复用的方式。
 * 依然是实际刷盘之后才会向producer返回确认
 * @author yilun.fyl
 *
 */
public class FsyncService extends Thread{
	
	Queue<AckWaitingEntry> waitingQueue;
	
	public FsyncService() {
		waitingQueue = new ConcurrentLinkedQueue<AckWaitingEntry>();
		start();
	}
	
	public void put(AckWaitingEntry entry){
		waitingQueue.add(entry);
	}

	@Override
	public void run() {
		while(true){
			if(!waitingQueue.isEmpty()){
				DataHelper.force();
			}
			while(!waitingQueue.isEmpty()){
				AckWaitingEntry entry = waitingQueue.poll();
				entry.ctx.writeAndFlush(entry.result);
			}
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
			}
		}
	}

}
