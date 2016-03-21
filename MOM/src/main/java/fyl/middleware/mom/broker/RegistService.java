package fyl.middleware.mom.broker;

import io.netty.channel.ChannelHandlerContext;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import fyl.middleware.mom.api.ConsumeResult;
import fyl.middleware.mom.api.MessageExt;
import fyl.middleware.mom.api.SendResult;
import fyl.middleware.mom.api.SendStatus;
import fyl.middleware.mom.data.DataHelper;
import fyl.middleware.mom.data.FileManager;
import fyl.middleware.mom.utils.CollectionUtils;

public class RegistService {

	private Map<String/* topic */, Set<String/* groupId */>> brokerMap;
	private Map<String/* groupId */, Set<MomServerHandler>> groupRouter;
	private int msgIndex; // 触发条件要求并不严格，无需用AtomicInteger
	private FsyncService fsyncService;
	private Random r;
	private ReSendService resendService;
	private ServerConfig serverConfig;
	private List<MessageExt> recoverList;

	public RegistService(ServerConfig serverConfig) {
		this.serverConfig = serverConfig;
		brokerMap = new ConcurrentHashMap<String, Set<String>>();
		groupRouter = new ConcurrentHashMap<String, Set<MomServerHandler>>();
		fsyncService = new FsyncService();
		resendService = new ReSendService(this, serverConfig);
		r = new Random();
		recoverList = FileManager.recoverUnsendMsg();
	}

	/**
	 * 重发尚未被消费的消息，服务器启动时调用
	 */
	public void recover() {
		for (MessageExt msg : recoverList) {
			sendMsgToConsumer(msg, msg.getGroupId(), msg.getStoreIndex(), 1);
		}
	}

	public void setRegist(MessageExt message, MomServerHandler channel) {
		String topic = message.getMessage().getTopic();
		String groupId = message.getGroupId();
		if (brokerMap.containsKey(topic)) {
			brokerMap.get(topic).add(groupId);
		} else {
			Set<String> groupSet = Collections
					.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
			groupSet.add(groupId);
			brokerMap.put(topic, groupSet);
		}
		if (groupRouter.containsKey(groupId)) {
			groupRouter.get(groupId).add(channel);
		} else {
			Set<MomServerHandler> chennelSet = Collections
					.newSetFromMap(new ConcurrentHashMap<MomServerHandler, Boolean>());
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
			// 由于broker并不保存历史订阅关系，可能是因为1.消费者掉线 2.broker刚刚重启，消费者尚未重连
			DataHelper.saveMessage(message);
			sendMsgToConsumer(message, null, message.getStoreIndex(), 1);
			return;
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
		if (msgIndex > serverConfig.getFSYNC_COMMIT_COUNT()) {
			fsyncService.interrupt();
			msgIndex = 0;
		} else {
			msgIndex++;
		}
		Iterator<Entry<String, Long>> mapIterator = indexMap.entrySet()
				.iterator();
		while (mapIterator.hasNext()) {
			Entry<String, Long> entry = mapIterator.next();
			sendMsgToConsumer(message, entry.getKey(), entry.getValue(), 1);
		}
		return;
	}

	public boolean sendMsgToConsumer(MessageExt message, String groupId,
			Long index, int sendCount) {
		if (groupId == null) {// 说明发送时 无消费者连接
			Set<String> groupSet = brokerMap.get(message.getMessage().getTopic());
			if(CollectionUtils.isEmpty(groupSet)){
				resendService.put(new MsgPendingEntry(message.getMsgId(), groupId,
						sendCount), message);
				return false;
			}
			Iterator<String> i = groupSet.iterator();
			boolean succ = true;
			while(i.hasNext()){
				String groupTmp = i.next();
				succ = sendMsgToConsumer(message, groupTmp,
						message.getStoreIndex(), sendCount) && succ;
			}
			return succ;
		}
		message.setStoreIndex(index);
		message.setGroupId(groupId);
		Set<MomServerHandler> channels = groupRouter.get(groupId);
		if (channels==null || channels.size() == 0) {
			resendService.put(new MsgPendingEntry(message.getMsgId(), groupId,
					sendCount), message);
			return false;
		}
		MomServerHandler[] channelArr = channels
				.toArray(new MomServerHandler[0]);
		try {
			MomServerHandler channel = channelArr[r.nextInt(channelArr.length)];
			channel.sendMsgToConsumer(message);
		} catch (Exception e) {
			// 这里产生的问题可能是1.突然连接断了，数组越界 2.发送失败
			e.printStackTrace();
			// 等待重发
			resendService.put(new MsgPendingEntry(message.getMsgId(), groupId,
					sendCount), message);
			return false;
		}
		resendService.put(new MsgPendingEntry(message.getMsgId(), groupId,
				sendCount), message);
		return true;
	}

	private SendResult sendResult(MessageExt message, boolean succ) {
		SendResult sendResult = new SendResult();
		sendResult.setMsgId(message.getMsgId());
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

	public void receivedConsumeResult(ConsumeResult result) {
		resendService.remove(new MsgPendingEntry(result.getMsgId(), result
				.getGroupId()));
	}
}
