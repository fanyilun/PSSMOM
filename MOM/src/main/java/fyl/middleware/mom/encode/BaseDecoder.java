package fyl.middleware.mom.encode;

import io.netty.buffer.ByteBuf;

import java.util.Map;

public class BaseDecoder {

	protected void readMap(ByteBuf frame, Map<String, String> propertyMap) {
		int length = frame.readInt();
		for (int i = 0; i < length; i++) {
			String key = readString(frame);
			String value = readString(frame);
			propertyMap.put(key, value);
		}
	}

	protected String readString(ByteBuf frame) {
		int length = frame.readInt();
		if(length<0){
			return null;
		}
		byte[] arr = new byte[length];
		frame.readBytes(arr);
		return new String(arr);
	}
	
	protected byte[] readByteArray(ByteBuf frame){
		int length = frame.readInt();
		if(length<0){
			return null;
		}
		byte[] arr = new byte[length];
		frame.readBytes(arr);
		return arr;
	}
	
}
