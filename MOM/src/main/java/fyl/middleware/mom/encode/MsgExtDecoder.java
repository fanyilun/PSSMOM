package fyl.middleware.mom.encode;

import io.netty.buffer.ByteBuf;
import fyl.middleware.mom.api.Message;
import fyl.middleware.mom.api.MessageExt;
import fyl.middleware.mom.api.MsgID;

/**
 * 自定义MessageExt的序列化协议
 * @author yilun.fyl
 *
 */
public class MsgExtDecoder extends BaseDecoder{

	public MessageExt decode(ByteBuf frame) {
		Message msg = new Message();
		MessageExt msgExt = new MessageExt(msg);
		msgExt.setMsgId(new MsgID(readMsgID(frame)));
		msgExt.setType(frame.readByte());
		msgExt.setAction(frame.readByte());
		msgExt.setGroupId(readString(frame));
		msgExt.setFilter(readString(frame));
		msgExt.setStoreIndex(frame.readLong());
		if(frame.readBoolean()){
			return msgExt;
		}
		
		msg.setTopic(readString(frame));
		msg.setBornTime(frame.readLong());
		msg.setBody(readByteArray(frame));
		readMap(frame,msg.getPropertyMap());
		return msgExt;
	}
	
}
