package fyl.middleware.mom.producer;

import fyl.middleware.mom.api.Message;
import fyl.middleware.mom.api.MessageExt;
import fyl.middleware.mom.api.Producer;
import fyl.middleware.mom.api.SendCallback;
import fyl.middleware.mom.api.SendResult;
import fyl.middleware.mom.api.SendStatus;
import fyl.middleware.mom.broker.MessageCenter;

public class DefaultProducer implements Producer {

	private String groupId;
	private String topic;
	private ProducerConection producerConection;

	/**
	 * 启动生产者，初始化底层资源。在所有属性设置完毕后，才能调用这个方法
	 */
	public void start() {
		String brokerIp = System.getProperty("SIP");
		if (brokerIp == null) {
			brokerIp = "127.0.0.1";
		}

		producerConection = new ProducerConection();

		producerConection.setBrokerIp(brokerIp);

		try {
			producerConection.connect();
		} catch (Throwable throwable) {
			throwable.printStackTrace();
		}
	}

	/**
	 * 设置生产者可发送的topic
	 * 
	 * @param topic
	 */
	public void setTopic(String topic) {
		this.topic = topic;
	}

	/**
	 * 设置生产者id，broker通过这个id来识别生产者集群
	 * 
	 * @param groupId
	 */
	public void setGroupId(String groupId) {
		this.groupId = groupId;
	}

	/**
	 * 发送消息
	 * 
	 * @param message
	 * @return
	 */
	public SendResult sendMessage(Message message) {
		message.setTopic(topic);
		message.setMsgId(MessageCenter.generateId());// TODO 全局唯一
		message.setBornTime(System.currentTimeMillis());
		MessageExt msgExt = new MessageExt(message);
		msgExt.setGroupId(groupId);
		try {
			return producerConection.sendMessage(msgExt);
		} catch (Throwable throwable) {
			throwable.printStackTrace();
		}
		SendResult result = new SendResult();
		result.setMsgId(message.getMsgId());
		result.setStatus(SendStatus.FAIL);
		return result;
	}

	/**
	 * 异步callback发送消息，当前线程不阻塞。broker返回ack后，触发callback
	 * 
	 * @param message
	 * @param callback
	 */
	public void asyncSendMessage(Message message, SendCallback callback) {
		message.setTopic(topic);
		message.setMsgId(MessageCenter.generateId());
		message.setBornTime(System.currentTimeMillis());
		MessageExt msgExt = new MessageExt(message);
		msgExt.setGroupId(groupId);
		producerConection.asyncSendMessage(msgExt, callback);
	}

	/**
	 * 停止生产者，销毁资源
	 */
	public void stop() {
		producerConection.close();
	}

}
