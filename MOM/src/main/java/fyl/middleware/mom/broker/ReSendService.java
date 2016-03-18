package fyl.middleware.mom.broker;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * 用于重发消息
 * 消息超过指定时间仍未被消费，则会被认为是死信，直接保存到文件中
 * @author yilun.fyl
 *
 */
public class ReSendService extends Thread{
	
	private Queue<ReSendWaitingEntry> waitingQueue;
	private static final int sendInterval = 8;
	
	public ReSendService() {
		waitingQueue = new ConcurrentLinkedQueue<ReSendWaitingEntry>();
		start();
	}
	
	public void put(ReSendWaitingEntry entry){
		waitingQueue.add(entry);
	}

	@Override
	public void run() {
		while(true){
			try {
				Thread.sleep(sendInterval*1000);
			} catch (InterruptedException e) {
			}
		}
	}

}
