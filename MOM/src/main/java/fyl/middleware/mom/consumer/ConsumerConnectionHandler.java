package fyl.middleware.mom.consumer;

import fyl.middleware.mom.api.ConsumeResult;
import fyl.middleware.mom.api.Message;
import fyl.middleware.mom.api.MessageExt;
import fyl.middleware.mom.api.MessageListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;

import java.util.concurrent.TimeUnit;

public class ConsumerConnectionHandler extends ChannelInboundHandlerAdapter {

	/**
	 * 缓存订阅消息，用于断线重连时发送
	 */
	private MessageExt subscribeMsg;

	private String groupId;

	/**
	 * 用户处置消息的回调方法
	 */
	private MessageListener listener;
	private long latestHeartBeatReceive;
	private long latestHeartBeatSend;
	private boolean isReconnecting;
	private ConsumerConnection consumerConnection;

	public ConsumerConnectionHandler(MessageListener listener,
			MessageExt subscribeMsg, ConsumerConnection consumerConnection) {
		this.groupId = subscribeMsg.getGroupId();
		this.listener = listener;
		this.subscribeMsg = subscribeMsg;
		this.consumerConnection = consumerConnection;
	}


	@Override
	public void channelActive(final ChannelHandlerContext ctx) throws Exception {
		if (subscribeMsg != null) {
			// 首次启动发送订阅消息
			ctx.writeAndFlush(subscribeMsg);
		}
		// 第一次启动心跳定时器
		latestHeartBeatReceive = System.currentTimeMillis();
		final Timer timer = new HashedWheelTimer();
		timer.newTimeout(new TimerTask() {

			@Override
			public void run(Timeout arg0) throws Exception {
				long currTime = System.currentTimeMillis();
				if (currTime - latestHeartBeatReceive > 8000
						&& currTime - latestHeartBeatSend < 8000) {
					// 意味着断线了，我们需要重连
					tryReconnect();
					ctx.close();
				} else {
					timer.newTimeout(this, 8, TimeUnit.SECONDS);
				}
			}
		}, 8, TimeUnit.SECONDS);

	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg)
			throws Exception {
		MessageExt message = (MessageExt) msg;
		if (message.getType()==MessageExt.TYPE_PRODUCER) {
			// 返回确认
			ConsumeResult consumeResult = listener.onMessage(message.getMessage());
			consumeResult.setInfo(String.valueOf(message.getStoreIndex()));//TODO 换个字段
			ctx.writeAndFlush(consumeResult);
		} else if (message.getType()==MessageExt.TYPE_BROKER) {
			// 成功接收到ping
			latestHeartBeatReceive = System.currentTimeMillis();
		}
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
			throws Exception {
		cause.printStackTrace();
		ctx.close();
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		if (!consumerConnection.isPermitClose()) {
			tryReconnect();
		}
	}

	@Override
	public void userEventTriggered(ChannelHandlerContext ctx, Object evt)
			throws Exception {
		if (evt instanceof IdleStateEvent) {
			IdleStateEvent e = (IdleStateEvent) evt;
			switch (e.state()) {
			case WRITER_IDLE:
				Message msg = new Message();
				msg.setBornTime(System.currentTimeMillis());
				MessageExt msgext = new MessageExt(msg);
				msgext.setType(MessageExt.TYPE_CONSUMER);
				msgext.setAction(MessageExt.ACTION_PING);
				msgext.setGroupId(groupId);
				System.out.println("consumer send heart beat");
				ctx.writeAndFlush(msgext);
				latestHeartBeatSend = System.currentTimeMillis();
				break;
			default:
				break;
			}
		}
	}

	private void tryReconnect() {
		if (isReconnecting) {
			return;
		}
		isReconnecting = true;
		while (true) {
			try {
				consumerConnection.connect(subscribeMsg, listener);
				break;
			} catch (Throwable e) {
				// 说明失败了，需要继续尝试
				e.printStackTrace();
				try {
					Thread.sleep(3000);
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}
			}
		}
		isReconnecting = false;
	}
}
