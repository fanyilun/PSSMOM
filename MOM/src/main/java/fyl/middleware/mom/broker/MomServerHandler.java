package fyl.middleware.mom.broker;

import fyl.middleware.mom.api.ConsumeResult;
import fyl.middleware.mom.api.MessageExt;
import fyl.middleware.mom.data.DataHelper;
import fyl.middleware.mom.utils.StringUtils;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;

import java.util.concurrent.TimeUnit;

/**
 */
public class MomServerHandler extends ChannelInboundHandlerAdapter {

	private RegistService registserver;
	private ChannelHandlerContext myCtx;
	private String filterKey;
	private String filterValue;
	private MessageExt lastMsg;
	private long latestHeartBeat;
	private Timer timer;

	public MomServerHandler(RegistService registserver) {
		this.registserver = registserver;
		registserver.initRandom();
	}

	public void setFilter(String filterKey, String filterValue) {
		this.filterKey = filterKey;
		this.filterValue = filterValue;
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg)
			throws Exception {
		if (msg instanceof MessageExt) {
			MessageExt message = (MessageExt) msg;
			lastMsg = message;
			if (message.getType() == MessageExt.TYPE_CONSUMER) {
				addHeartBreakReceiver();
				// 收到订阅消息
				if (message.getAction() == MessageExt.ACTION_SUBSCRIBE) {
					registserver.setRegist(message, this);
					if (!StringUtils.isBlank(message.getFilter())) {
						String[] conditions = message.getFilter().split("=");
						if (conditions.length > 1) {
							filterKey = conditions[0];
							filterValue = conditions[1];
						}
					}
				} else if (message.getAction() == MessageExt.ACTION_LOGOUT) {
					// 注销
					ctx.close();
				} else if (message.getAction() == MessageExt.ACTION_PING) {
					System.out.println("broker receive heart beat");
					latestHeartBeat = System.currentTimeMillis();
					// 应答
					MessageExt pingMsg = new MessageExt(null);
					pingMsg.setType(MessageExt.TYPE_BROKER);
					pingMsg.setAction(MessageExt.ACTION_PING);
					ctx.writeAndFlush(pingMsg);
				}
			} else if (message.getType() == MessageExt.TYPE_PRODUCER) {
				// producer发来的消息只有一种
				registserver.sendMsg(message, ctx);
			}
		} else if (msg instanceof ConsumeResult) {
			ConsumeResult result = (ConsumeResult) msg;
			registserver.receivedConsumeResult(result);
				DataHelper.deleteMessage(result
						.getStorageIndex(),result.getMsgId());
				// TODO 消费失败不再重发，而是交给业务去解决
		}
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
			throws Exception {
		cause.printStackTrace();
		ctx.close();
	}


	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		myCtx = ctx;
	}

	/**
	 * 需要超时重连嘛
	 */
	public void sendMsgToConsumer(MessageExt message) {
		if (filterValue != null && filterKey != null) {
			if (!filterValue
					.equals(message.getMessage().getProperty(filterKey))) {
				return;
			}
		}
		myCtx.writeAndFlush(message);
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		if (lastMsg != null && lastMsg.getType() == MessageExt.TYPE_CONSUMER) {
			registserver.disRegist(lastMsg, this);
		}
	}

	public void addHeartBreakReceiver() {
		// 第一次启动心跳定时器
		latestHeartBeat = System.currentTimeMillis();
		timer = new HashedWheelTimer();
		timer.newTimeout(new HeartBeatChecker(), 8, TimeUnit.SECONDS);
	}

	class HeartBeatChecker implements TimerTask {

		@Override
		public void run(Timeout arg0) throws Exception {
			if (System.currentTimeMillis() - latestHeartBeat > 8000) {
				// 意味着断线了，consumer会自动发起重连，我们需要关闭这个连接
				myCtx.close();
			} else {
				timer.newTimeout(this, 8, TimeUnit.SECONDS);
			}
		}

	}
}
