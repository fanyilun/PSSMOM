package fyl.middleware.mom.broker;

import fyl.middleware.mom.api.MsgID;

/**
 * 
 * @author yilun.fyl
 *
 */
public class MsgPendingEntry {

	private MsgID msgId;
	private String groupId;
	private int sentCount;//已重发次数
	private long transmitTime;//最近发送时间
	
	public MsgPendingEntry(MsgID msgId, String groupId,int sentCount) {
		this.msgId = msgId;
		this.groupId = groupId;
		this.sentCount= sentCount;
		transmitTime = System.currentTimeMillis();
	}
	
	public MsgPendingEntry(MsgID msgId, String groupId) {
		this.msgId = msgId;
		this.groupId = groupId;
	}
	
	
	public int getSentCount() {
		return sentCount;
	}

	public MsgID getMsgId() {
		return msgId;
	}

	public String getGroupId() {
		return groupId;
	}

	public long getTransmitTime() {
		return transmitTime;
	}

	@Override
	public int hashCode() {
		if(groupId==null){
			return msgId.hashCode();
		}
		return msgId.hashCode()+groupId.hashCode();
	}
	
	@Override
	public boolean equals(Object obj) {
		MsgPendingEntry entry = (MsgPendingEntry)obj;
		return msgId.equals(entry.msgId) && groupId.equals(entry.groupId);
	}
}
