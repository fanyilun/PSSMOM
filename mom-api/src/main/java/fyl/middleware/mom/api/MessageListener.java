package fyl.middleware.mom.api;

public interface MessageListener {
	/**
	 * 
	 * @param message
	 * @return
	 */
	ConsumeResult onMessage(Message message);
}
