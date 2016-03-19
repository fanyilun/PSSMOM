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
	
	public void encode(ByteBuf out, Object msg) {
		ConsumeResult consumeResult = (ConsumeResult) msg;
		out.writeByte(3); //3表示ConsumeResult
		writeString(out, consumeResult.getInfo());
		out.writeBoolean(consumeResult.getStatus()==ConsumeStatus.SUCCESS);
		writeByteArray(out, consumeResult.getMsgId().getIdData());
		out.writeLong(consumeResult.getStorageIndex());
		writeString(out, consumeResult.getGroupId());
	}
	
}
