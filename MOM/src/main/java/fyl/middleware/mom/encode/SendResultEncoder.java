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
	
	public void encode(ByteBuf out, Object msg) {
		SendResult sendResult = (SendResult) msg;
		out.writeByte(2); //2表示SendResult
		writeByteArray(out, sendResult.getMsgId().getIdData());
		writeString(out, sendResult.getInfo());
		out.writeBoolean(sendResult.getStatus()==SendStatus.SUCCESS);
	}
}
