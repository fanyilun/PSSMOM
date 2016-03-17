package fyl.middleware.mom.encode;

import fyl.middleware.mom.api.SendResult;
import fyl.middleware.mom.api.SendStatus;
import io.netty.buffer.ByteBuf;

/**
 * 自定义SendResult的序列化协议
 * @author yilun.fyl
 *
 */
public class SendResultEncoder extends BaseEncoder{
	
	public SendResultEncoder() {
	}

	public void encode(ByteBuf out, Object msg) {
		SendResult sendResult = (SendResult) msg;
		
		out.writeInt(0); //对象长度 占位用
		out.writeByte(2); //2表示SendResult
		writeString(out, sendResult.getMsgId());
		writeString(out, sendResult.getInfo());
		out.writeBoolean(sendResult.getStatus()==SendStatus.SUCCESS);
	}
}
