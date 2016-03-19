package fyl.middleware.mom.broker;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import fyl.middleware.mom.api.MessageExt;

/**
 * 用于重发消息 消息超过指定时间仍未被消费，则会被认为是死信，直接保存到文件中
 * 
 * @author yilun.fyl
 * 
 */
public class ReSendService extends Thread {

	/**
	 * 刷新频率（也是最小重发时间）
	 */
	private static final int sendInterval = 10;

	/**
	 * 最大重发次数
	 */
	private static final int maxResendCount = 10;
	private RegistService registService;

	private Map<MsgPendingEntry, MessageExt> pendingMap;// 存放已发送但未收到ConsumeResult的消息

	public ReSendService(RegistService registService) {
		this.registService = registService;
		pendingMap = new ConcurrentHashMap<MsgPendingEntry, MessageExt>();
		start();
	}

	@Override
	public void run() {
		while (true) {
			Iterator<Entry<MsgPendingEntry, MessageExt>> i = pendingMap
					.entrySet().iterator();
			long currentTime = System.currentTimeMillis();
			while (i.hasNext()) {
				Entry<MsgPendingEntry, MessageExt> entry = i.next();
				MsgPendingEntry pendingEntry = entry.getKey();
				if (currentTime - pendingEntry.getTransmitTime() > Math.pow(
						2, 2 + pendingEntry.getSentCount()) * 1000) {
					if(pendingEntry.getSentCount()>=maxResendCount){
						//TODO 成为死信 不重发了
						System.out.println("死信");
						i.remove();
					}else{
						registService.sendMsgToConsumer(entry.getValue(), pendingEntry.getGroupId(), entry.getValue().getStoreIndex(), pendingEntry.getSentCount()+1);
						i.remove();
					}
				}
			}
			try {
				Thread.sleep(sendInterval * 1000);
			} catch (InterruptedException e) {
			}
		}
	}

	public void put(MsgPendingEntry msgPendingEntry, MessageExt message) {
		pendingMap.put(msgPendingEntry, message);
	}

	public void remove(MsgPendingEntry msgPendingEntry) {
		pendingMap.remove(msgPendingEntry);
	}
}
