package fyl.middleware.mom.api;

public class SendResult {
	public String getInfo() {
		return info;
	}
	public void setInfo(String info) {
		this.info = info;
	}
	public SendStatus getStatus() {
		return status;
	}
	public void setStatus(SendStatus status) {
		this.status = status;
	}
	public MsgID getMsgId() {
		return msgId;
	}
	public void setMsgId(MsgID msgId) {
		this.msgId = msgId;
	}
	private String info;
	private SendStatus status;
	private MsgID msgId;
	@Override
	public String toString(){
		return "msg "+msgId+"  send "+(status==SendStatus.SUCCESS?"success":"fail")+"   info:"+info;
	}
	
}
