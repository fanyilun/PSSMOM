package fyl.middleware.mom.encode;

import io.netty.buffer.ByteBuf;

import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

public class BaseEncoder {

	private static Charset charset=Charset.forName("utf-8");
	
	protected void writeString(ByteBuf out, String s){
		if(s==null){
			out.writeInt(-1);
			return;
		}
		out.writeInt(s.length());
		out.writeBytes(s.getBytes(charset));
	}
	
	protected void writeByteArray(ByteBuf out, byte[] arr){
		if(arr==null){
			out.writeInt(-1);
			return;
		}
		out.writeInt(arr.length);
		out.writeBytes(arr);
	}
	
	/**
	 * MsgId固定16字节长度
	 * @param out
	 * @param arr
	 */
	protected void writeMsgId(ByteBuf out, byte[] arr){
		out.writeBytes(arr);
	}

	protected void writeMap(ByteBuf out, Map<String,String> map){
		if(map==null){
			out.writeInt(-1);
			return;
		}
		out.writeInt(map.size());
		Iterator<Entry<String,String>>  i = map.entrySet().iterator();
		while(i.hasNext()){
			Entry<String,String> entry = i.next();
			writeString(out, entry.getKey());
			writeString(out, entry.getValue());
		}
	}
}
