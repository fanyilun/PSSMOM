package fyl.middleware.mom.encode;

import fyl.middleware.mom.api.Message;
import fyl.middleware.mom.api.MessageExt;
import io.netty.buffer.ByteBuf;

/**
 * 自定义MessageExt的序列化协议
 * @author yilun.fyl
 *
 */
public class MsgExtDecoder extends BaseDecoder{

	public Object decode(ByteBuf frame) {
		Message msg = new Message();
		MessageExt msgExt = new MessageExt(msg);
		msgExt.setType(frame.readByte());
		msgExt.setAction(frame.readByte());
		msgExt.setGroupId(readString(frame));
		msgExt.setFilter(readString(frame));
		msgExt.setStoreIndex(frame.readLong());
		if(frame.readBoolean()){
			return msgExt;
		}
		msg.setMsgId(readString(frame));
		msg.setTopic(readString(frame));
		msg.setBornTime(frame.readLong());
		msg.setBody(readByteArray(frame));
		readMap(frame,msg.getPropertyMap());
		return msgExt;
	}

	
}
