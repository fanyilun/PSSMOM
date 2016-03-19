package fyl.middleware.mom.encode;

import fyl.middleware.mom.api.ConsumeResult;
import fyl.middleware.mom.api.ConsumeStatus;
import fyl.middleware.mom.api.MsgID;
import io.netty.buffer.ByteBuf;

/**
 * 自定义ConsumeResult的序列化协议
 * @author yilun.fyl
 *
 */
public class ConsumeResultDecoder extends BaseDecoder{

	public Object decode(ByteBuf frame) {
		ConsumeResult result = new ConsumeResult();
		result.setInfo(readString(frame));
		result.setStatus(frame.readBoolean()?ConsumeStatus.SUCCESS:ConsumeStatus.FAIL);
		result.setMsgId(new MsgID(readByteArray(frame)));
		result.setStorageIndex(frame.readLong());
		result.setGroupId(readString(frame));
		return result;
	}

}
