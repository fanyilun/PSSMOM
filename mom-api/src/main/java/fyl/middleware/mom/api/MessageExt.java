package fyl.middleware.mom.api;

import java.io.Serializable;

public class MessageExt implements Serializable {

	public transient static final byte TYPE_CONSUMER = 1;
	public transient static final byte TYPE_PRODUCER = 2;
	public transient static final byte TYPE_BROKER = 3;

	public transient static final byte ACTION_SUBSCRIBE = 1;
	public transient static final byte ACTION_LOGOUT = 2;
	public transient static final byte ACTION_PING = 3;

	private Message message;
	private byte type;
	private byte action;
	private String groupId;
	private String filter;
	private long storeIndex;
	//全局唯一的消息id，不同消息不能重复
	private MsgID msgId;


	public MessageExt(Message msg) {
		message = msg;
	}

	public Message getMessage() {
		return message;
	}

	public short getType() {
		return type;
	}
	public MsgID getMsgId() {
		return msgId;
	}
	public void setMsgId(MsgID msgId) {
		this.msgId = msgId;
	}
	public void setType(byte type) {
		this.type = type;
	}

	public short getAction() {
		return action;
	}

	public void setAction(byte action) {
		this.action = action;
	}

	public String getGroupId() {
		return groupId;
	}

	public void setGroupId(String groupId) {
		this.groupId = groupId;
	}

	public String getFilter() {
		return filter;
	}

	public void setFilter(String filter) {
		this.filter = filter;
	}

	public long getStoreIndex() {
		return storeIndex;
	}

	public void setStoreIndex(long storeIndex) {
		this.storeIndex = storeIndex;
	}
	
}
