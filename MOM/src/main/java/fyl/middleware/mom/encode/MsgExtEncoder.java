package fyl.middleware.mom.encode;

import fyl.middleware.mom.api.Message;
import fyl.middleware.mom.api.MessageExt;
import io.netty.buffer.ByteBuf;

/**
 * 自定义MessageExt的序列化协议
 * @author yilun.fyl
 *
 */
public class MsgExtEncoder extends BaseEncoder{

	public void encode(ByteBuf out, Object msg) {
		MessageExt msgExt = (MessageExt) msg;
		
		out.writeInt(0); //对象长度 占位用
		out.writeByte(1); //1表示MessageExt
		out.writeByte(msgExt.getType());
		out.writeByte(msgExt.getAction());
		writeString(out, msgExt.getGroupId());
		writeString(out, msgExt.getFilter());
		out.writeLong(msgExt.getStoreIndex());
		Message message=msgExt.getMessage();
		out.writeBoolean(message==null);//尽管是boolean实际还是占位1字节
		if(message==null){
			return;
		}
		writeString(out, message.getMsgId());
		writeString(out, message.getTopic());
		out.writeLong(message.getBornTime());
		writeByteArray(out, message.getBody());
		writeMap(out, message.getPropertyMap());
	}
	
	
}
