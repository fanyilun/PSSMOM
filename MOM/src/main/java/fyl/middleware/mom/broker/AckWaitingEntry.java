package fyl.middleware.mom.broker;

import fyl.middleware.mom.api.SendResult;
import io.netty.channel.ChannelHandlerContext;

public class AckWaitingEntry {
	
	ChannelHandlerContext ctx;
	SendResult result;

	public AckWaitingEntry(ChannelHandlerContext ctx, SendResult result) {
		this.ctx = ctx;
		this.result = result;
	}
}
