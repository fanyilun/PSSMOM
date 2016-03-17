package fyl.middleware.mom.consumer;

/**
 * 消息消费异常
 * @author yilun.fyl
 *
 */
public class ConsumerException extends Exception{
	
	private static final long serialVersionUID = -3868156127873246778L;
	
	private String detail;
	public ConsumerException(String detail) {
		this.detail = detail;
	}

	@Override
	public String toString() {
		return "ConsumerException["+detail+"]";
	}
}
