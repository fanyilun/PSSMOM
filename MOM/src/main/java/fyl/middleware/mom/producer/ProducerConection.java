package fyl.middleware.mom.producer;

import fyl.middleware.mom.api.MessageExt;
import fyl.middleware.mom.api.MsgID;
import fyl.middleware.mom.api.SendCallback;
import fyl.middleware.mom.api.SendResult;
import fyl.middleware.mom.encode.MyDecoder;
import fyl.middleware.mom.encode.MyEncoder;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

public class ProducerConection extends ChannelInboundHandlerAdapter {

	private String brokerIp;
	public static final int PORT = 9999;
	// 事实证明，CountDownLatch性能要优于Thread.interrupt
	private Map<MsgID, CountDownLatch> waitingThread = new ConcurrentHashMap<MsgID, CountDownLatch>();
	private Map<MsgID, SendResult> sendResult = new ConcurrentHashMap<MsgID, SendResult>();
	private Channel channel;

	private boolean connected;

	public void setBrokerIp(String brokerIp) {
		this.brokerIp = brokerIp;
	}

	private SendResult getSendResult(MsgID msgId) {
		return sendResult.get(msgId);
	}

	private void setSendResult(MsgID msgId, SendResult result) {
		sendResult.put(msgId, result);
	}

	private void removeSendResult(MsgID msgId) {
		sendResult.remove(msgId);
		waitingThread.remove(msgId);
	}

	public SendResult sendMessage(MessageExt msgExt) throws Throwable {
		msgExt.setType(MessageExt.TYPE_PRODUCER);
		if (!connected) {
			throw new IllegalStateException("not connected");
		}
		CountDownLatch c = new CountDownLatch(1);
		waitingThread.put(msgExt.getMsgId(), c);
		channel.writeAndFlush(msgExt);
		c.await();
		SendResult result = getSendResult(msgExt.getMsgId());
		removeSendResult(msgExt.getMsgId());
		return result;
	}

	SendCallback callback;

	public void asyncSendMessage(MessageExt message, final SendCallback callback) {
		message.setType(MessageExt.TYPE_PRODUCER);
		/** 如果处于非连接状态 */
		if (!connected) {
			throw new IllegalStateException("not connected");
		}

		/** 发送Message */
		channel.writeAndFlush(message);
		this.callback = callback;
	}

	public void connect() throws Throwable {
		EventLoopGroup group = new NioEventLoopGroup();
		try {
			Bootstrap bootstrap = new Bootstrap();
			bootstrap
					.group(group)
					.channel(NioSocketChannel.class)
					.remoteAddress(brokerIp, PORT)
					.option(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK,
							10 * 64 * 1024)
					.option(ChannelOption.SO_KEEPALIVE, true)
					.option(ChannelOption.TCP_NODELAY, true)
					.option(ChannelOption.SO_REUSEADDR, true)
					.option(ChannelOption.SO_SNDBUF, 65536)
					.option(ChannelOption.SO_RCVBUF, 65536)
					.handler(new ChannelInitializer<SocketChannel>() {

						@Override
						protected void initChannel(SocketChannel socketChannel)
								throws Exception {
							socketChannel.pipeline().addLast(new MyDecoder());
							socketChannel.pipeline().addLast(new MyEncoder());
							socketChannel.pipeline().addLast(
									ProducerConection.this);
						}
					});

			ChannelFuture channelFuture = bootstrap.connect().sync();

			if (!channelFuture.awaitUninterruptibly().isSuccess()) {
				throw new Exception("connect fail");
			}

			connected = true;
			channel = channelFuture.channel();

		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}

	public void close() {
		connected = false;
		if (null != channel) {
			channel.close().awaitUninterruptibly();
			channel.eventLoop().shutdownGracefully();
			channel = null;
		}
	}

	public boolean isClosed() {
		return (null == channel) || !channel.isActive()
				|| !channel.isWritable();
	}

	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {

	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg)
			throws Exception {

		if (msg instanceof SendResult) {
			SendResult result = (SendResult) msg;
			setSendResult(result.getMsgId(), result);
			CountDownLatch c = waitingThread.get(result.getMsgId());
			if (c != null) {
				c.countDown();
			} else {
				System.out.println("Error:MsgID repeated");
			}
			if (callback != null) {
				callback.onResult(result);
				callback = null;
			}
		}
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
			throws Exception {
		cause.printStackTrace();
		ctx.close();
	}

	@Override
	public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
		ctx.flush();
	}

}
