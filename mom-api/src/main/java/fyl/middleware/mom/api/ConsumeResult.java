package fyl.middleware.mom.api;

public class ConsumeResult {
	private ConsumeStatus status=ConsumeStatus.FAIL;
	private String info;
	private MsgID MsgId;
	private Long storageIndex;
	private String groupId;
	public void setStatus(ConsumeStatus status) {
		this.status = status;
	}
	public ConsumeStatus getStatus() {
		return status;
	}
	public void setInfo(String info) {
		this.info = info;
	}
	public String getInfo() {
		return info;
	}
	public void setGroupId(String groupId) {
		this.groupId = groupId;
	}
	public String getGroupId() {
		return groupId;
	}
	public MsgID getMsgId() {
		return MsgId;
	}
	public void setMsgId(MsgID msgId) {
		MsgId = msgId;
	}
	public Long getStorageIndex() {
		return storageIndex;
	}
	public void setStorageIndex(Long storageIndex) {
		this.storageIndex = storageIndex;
	}
}
