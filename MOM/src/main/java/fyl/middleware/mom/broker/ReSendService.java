package fyl.middleware.mom.broker;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import fyl.middleware.mom.api.MessageExt;
import fyl.middleware.mom.data.DataHelper;
import fyl.middleware.mom.data.FileManager;

/**
 * 用于重发消息 消息超过指定时间仍未被消费，则会被认为是死信，直接保存到文件中
 * 
 * @author yilun.fyl
 * 
 */
public class ReSendService extends Thread {

	private RegistService registService;

	private Map<MsgPendingEntry, MessageExt> pendingMap;// 存放已发送但未收到ConsumeResult的消息
	
	private ServerConfig serverConfig;

	public ReSendService(RegistService registService, ServerConfig serverConfig) {
		this.registService = registService;
		this.serverConfig = serverConfig;
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
					if(pendingEntry.getSentCount()>=serverConfig.getMAX_RESEND_COUNT()){
						i.remove();
						//存入死信文件
						DataHelper.saveDeadMessage(entry.getValue());
						//死信会从持久化文件里删除
						FileManager.validateAndDelete(entry.getValue().getStoreIndex(), entry.getValue().getMsgId());
					}else{
						registService.sendMsgToConsumer(entry.getValue(), pendingEntry.getGroupId(), entry.getValue().getStoreIndex(), pendingEntry.getSentCount()+1);
						i.remove();
					}
				}
			}
			try {
				Thread.sleep(serverConfig.getRESEND_INTERVAL() * 1000);
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
