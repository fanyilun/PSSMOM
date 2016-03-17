package fyl.middleware.mom.encode;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

public class MyDecoder extends LengthFieldBasedFrameDecoder {

	private final MsgExtDecoder msgExtDecoder;
	private final SendResultDecoder sendResultDecoder;
	private final ConsumeResultDecoder consumeResulDecoder;

	public MyDecoder() {
		super(Integer.MAX_VALUE, 0, 4, 0, 4);
		this.msgExtDecoder = new MsgExtDecoder();
		sendResultDecoder = new SendResultDecoder();
		consumeResulDecoder = new ConsumeResultDecoder();
	}

	@Override
	protected Object decode(final ChannelHandlerContext ctx, final ByteBuf in)
			throws Exception {
		ByteBuf frame = (ByteBuf) super.decode(ctx, in);
		if (frame == null) {
			return null;
		}
		try {
			// 长度头已经去掉了
			byte classType = frame.readByte();
			switch (classType) {
			case 1:
				return msgExtDecoder.decode(frame);
			case 2:
				return sendResultDecoder.decode(frame);
			case 3:
				return consumeResulDecoder.decode(frame);
			}
			return null;
		} finally {
			if (null != frame) {
				frame.release();
			}
		}
	}
}
