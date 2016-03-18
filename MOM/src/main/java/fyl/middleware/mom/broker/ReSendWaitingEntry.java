package fyl.middleware.mom.broker;

import fyl.middleware.mom.api.MessageExt;

/**
 * 待重发的消息实体
 * @author yilun.fyl
 *
 */
public class ReSendWaitingEntry {
	
	MessageExt msg;
	int sentCount;//已重发的次数

	public ReSendWaitingEntry(MessageExt msg) {
		this.msg = msg;
	}
	
}
