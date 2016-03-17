package fyl.middleware.mom.consumer;

import fyl.middleware.mom.api.Consumer;
import fyl.middleware.mom.api.Message;
import fyl.middleware.mom.api.MessageExt;
import fyl.middleware.mom.api.MessageListener;

/**
 * 消息消费者对象 由消息的消费方使用
 * 
 * @author yilun.fyl
 * 
 */
public class DefaultConsumer implements Consumer {

	
	private String groupId;
	private MessageListener listener;
	private ConsumerConnection consumerConnection;
	private MessageExt subscribeMsg;

	public void start() {
		if(subscribeMsg==null){
			System.out.println("ERROR:you must subscribe first");
			return;
//			throw new ConsumerException("you must subscribe first");
		}
		if(consumerConnection!=null && !consumerConnection.isPermitClose()){
			System.out.println("ERROR:you must close the connection first");
			return;
//			throw new ConsumerException("you must close the connection first");
		}
		String brokerIp = System.getProperty("SIP");
		if (brokerIp == null) { //仅供测试用
			brokerIp = "127.0.0.1"; 
		}
		consumerConnection = new ConsumerConnection(brokerIp);
		try {
			consumerConnection.connect(subscribeMsg, listener);
		} catch (Throwable throwable) {
			throwable.printStackTrace();
		}
	}

	@Override
	public void subscribe(String topic, String filter,
			MessageListener listener){
		if(groupId==null){
			System.out.println("ERROR:consumer groupId must be set");
			return;
//			throw new ConsumerException("consumer groupId must be set");
		}
		Message msg = new Message();
		subscribeMsg = new MessageExt(msg);
		msg.setTopic(topic);
		subscribeMsg.setGroupId(groupId);
		subscribeMsg.setAction(MessageExt.ACTION_SUBSCRIBE);
		subscribeMsg.setType(MessageExt.TYPE_CONSUMER);
		subscribeMsg.setFilter(filter);
		this.listener = listener;
	}

	public void setGroupId(String groupId) {
		this.groupId = groupId;
	}

	public void stop() {
		consumerConnection.close();
	}

}
