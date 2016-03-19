package fyl.middleware.mom.api;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class Message implements Serializable{

	private String topic;
	private byte[] body;
	
	private Map<String, String> properties = new HashMap<String, String>();

	private long bornTime;
	
	public void setTopic(String topic) {
		this.topic = topic;
	}
	
	public String getTopic() {
		return topic;
	}

	public void setBody(byte[] body) {
		this.body = body;
	}

	public byte[] getBody() {
		return body;
	}

	public Map<String, String> getPropertyMap() {
		return properties;
	}
	
	public String getProperty(String key) {
		return properties.get(key);
	}
	/**
	 * 设置消息属性
	 * @param key
	 * @param value
	 */
	public void setProperty(String key, String value) {
		properties.put(key, value);
	}
	/**
	 * 删除消息属性
	 * @param key
	 */
	public void removeProperty(String key) {
		properties.remove(key);
	}

	public long getBornTime() {
		return bornTime;
	}
	public void setBornTime(long bornTime) {
		this.bornTime = bornTime;
	}

}
