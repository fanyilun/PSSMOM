package fyl.middleware.mom.encode;

import fyl.middleware.mom.api.MsgID;
import fyl.middleware.mom.api.SendResult;
import fyl.middleware.mom.api.SendStatus;
import io.netty.buffer.ByteBuf;

/**
 * 自定义SendResult的序列化协议
 * @author yilun.fyl
 *
 */
public class SendResultDecoder extends BaseDecoder{

	public Object decode(ByteBuf frame) {
		SendResult result = new SendResult();
		result.setMsgId(new MsgID(readByteArray(frame)));
		result.setInfo(readString(frame));
		result.setStatus(frame.readBoolean()?SendStatus.SUCCESS:SendStatus.FAIL);
		return result;
	}

}
