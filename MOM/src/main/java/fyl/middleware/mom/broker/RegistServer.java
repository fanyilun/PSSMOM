package fyl.middleware.mom.broker;

import io.netty.channel.ChannelHandlerContext;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import fyl.middleware.mom.api.MessageExt;
import fyl.middleware.mom.api.SendResult;
import fyl.middleware.mom.api.SendStatus;
import fyl.middleware.mom.data.DataHelper;

public class RegistServer {

	private Map<String/*topic*/, Set<String/*groupId*/>> brokerMap;
	private Map<String/*groupId*/, Set<MomServerHandler>> groupRouter;
	private int msgIndex = 0;
	private static final int FSYNC_COMMIT_COUNT = 6;
	private FsyncService fsyncService;
	private Random r;
    
	public RegistServer() {
		brokerMap = new ConcurrentHashMap<String, Set<String>>();
		groupRouter = new ConcurrentHashMap<String, Set<MomServerHandler>>();
		fsyncService = new FsyncService();
		r=new Random();
	}

	public void setRegist(MessageExt message, MomServerHandler channel) {
		String topic = message.getMessage().getTopic();
		String groupId = message.getGroupId();
		if (brokerMap.containsKey(topic)) {
			brokerMap.get(topic).add(groupId);
		} else {
			HashSet<String> groupSet = new HashSet<String>();
			groupSet.add(groupId);
			brokerMap.put(topic, groupSet);
		}
		if (groupRouter.containsKey(groupId)) {
			groupRouter.get(groupId).add(channel);
		} else {
			Set<MomServerHandler> chennelSet = Collections.newSetFromMap(new ConcurrentHashMap<MomServerHandler,Boolean>());
			chennelSet.add(channel);
			groupRouter.put(groupId, chennelSet);
		}
	}

	public void sendMsg(MessageExt message, ChannelHandlerContext ctx) {
		String topic = message.getMessage().getTopic();
		Set<String> groupSet = brokerMap.get(topic);
		Map<String/* groupID */, Long/* storage index */> indexMap = new HashMap<String, Long>();
		if (groupSet == null) {
			// 没有人订阅
			System.out.println("no one registed");
//			return;
			// 但是根据测试用例来看，consumer订阅和producer发送消息是同时发起的
			// 由于consumer订阅时做的事情多一点 经常导致先收到peoducer的消息 这样就发送失败了
			try {
				Thread.sleep(50);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			if ((groupSet = brokerMap.get(topic)) == null) {
				return;
			}
		}
		Iterator<String> i = groupSet.iterator();
		boolean succ = true;
		while (i.hasNext()) {
			String groupId = i.next();
			message.setGroupId(groupId);
			long index = DataHelper.saveMessage(message);
			if (index < 0) {
				succ = false;
				break;
			}
			indexMap.put(groupId, index);
		}
		fsyncService.put(new AckWaitingEntry(ctx, sendResult(message, succ)));
		if(msgIndex<FSYNC_COMMIT_COUNT){
			msgIndex++;
		}else{
			fsyncService.interrupt();
			msgIndex=0;
		}
		Iterator<Entry<String, Long>> mapIterator = indexMap.entrySet()
				.iterator();
		while (mapIterator.hasNext()) {
			Entry<String, Long> entry = mapIterator.next();
			message.setStoreIndex(entry.getValue());
			message.setGroupId(entry.getKey());
			Set<MomServerHandler> channels = groupRouter.get(entry.getKey());
			if (channels.size() == 0) {
				continue;
			}
			MomServerHandler[] channelArr = channels
					.toArray(new MomServerHandler[0]);
			MomServerHandler channel = channelArr[r.nextInt(channelArr.length)];
			channel.sendMsgToConsumer(message);
		}
		return;
	}

	private SendResult sendResult(MessageExt message, boolean succ) {
		SendResult sendResult = new SendResult();
		sendResult.setMsgId(message.getMessage().getMsgId());
		sendResult.setStatus(succ ? SendStatus.SUCCESS : SendStatus.FAIL);
		return sendResult;
	}

	public void disRegist(MessageExt lastMsg, MomServerHandler momServerHandler) {
		String groupId = lastMsg.getGroupId();
		Set<MomServerHandler> handlerSet = groupRouter.get(groupId);
		if (handlerSet == null) {
			// It shouldn't be null in normal case
			return;
		}
		handlerSet.remove(momServerHandler);
	}

	public Map<String, Set<String>> getBrokerMap() {
		return brokerMap;
	}
}
