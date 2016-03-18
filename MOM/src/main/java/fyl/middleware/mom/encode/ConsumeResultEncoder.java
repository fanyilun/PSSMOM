package fyl.middleware.mom.encode;

import fyl.middleware.mom.api.ConsumeResult;
import fyl.middleware.mom.api.ConsumeStatus;
import io.netty.buffer.ByteBuf;

/**
 * 自定义ConsumeResult的序列化协议
 * @author yilun.fyl
 *
 */
public class ConsumeResultEncoder extends BaseEncoder{
	
	public ConsumeResultEncoder() {
	}

	public void encode(ByteBuf out, Object msg) {
		ConsumeResult consumeResult = (ConsumeResult) msg;
		
		out.writeInt(0); //对象长度 占位用
		out.writeByte(3); //3表示ConsumeResult
		writeString(out, consumeResult.getInfo());
		out.writeBoolean(consumeResult.getStatus()==ConsumeStatus.SUCCESS);
	}
	
}
