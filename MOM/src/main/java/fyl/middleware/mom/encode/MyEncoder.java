package fyl.middleware.mom.encode;


import fyl.middleware.mom.api.ConsumeResult;
import fyl.middleware.mom.api.MessageExt;
import fyl.middleware.mom.api.SendResult;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

@Sharable
public final class MyEncoder extends MessageToByteEncoder<Object> {
	
	private final MsgExtEncoder msgExtEncoder;
	private final SendResultEncoder sendResultEncoder;
	private final ConsumeResultEncoder consumeResulEncoder;
	
	public MyEncoder() {
		msgExtEncoder = new MsgExtEncoder();//TODO
		sendResultEncoder = new SendResultEncoder();
		consumeResulEncoder = new ConsumeResultEncoder();
	}
	
	@Override
	protected void encode(final ChannelHandlerContext ctx, final Object msg, final ByteBuf out) throws Exception {
		//对于结构化的对象传输，自定义对齐的方式要比普通序列化好
		int startIdx = out.writerIndex();
		out.writeInt(0); //对象长度 占位用
		if(msg instanceof MessageExt){
			msgExtEncoder.encode(out, msg);
		}else if(msg instanceof SendResult){
			sendResultEncoder.encode(out, msg);
		}else if(msg instanceof ConsumeResult){
			consumeResulEncoder.encode(out, msg);
		}
		int endIdx = out.writerIndex();
		out.setInt(startIdx, endIdx - startIdx - 4);
	}
}
