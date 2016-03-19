package fyl.middleware.mom.api;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * 通过UUID生成唯一ID，定长16个字节
 * @author yilun.fyl
 * 
 */
public class MsgID {

	private byte[] idData;

	public MsgID() {
		idData = generateId();
	}
	
	public MsgID(byte[] arr){
		idData=arr;
	}

	public byte[] getIdData() {
		return idData;
	}

	/**
	 * 截取高4位作为int值作为hashcode
	 */
	@Override
	public int hashCode() {
		int value = (int) ((idData[0] & 0xFF) | ((idData[1] & 0xFF) << 8)
				| ((idData[2] & 0xFF) << 16) | ((idData[3] & 0xFF) << 24));
		return value;
	}

	@Override
	public boolean equals(Object obj) {
		MsgID m2 = (MsgID) obj;
		for (int i = 0; i < 16; i++)
			if (idData[i] != m2.idData[i])
				return false;
		return true;
	}

	private byte[] generateId() {
		return getIdAsByte(UUID.randomUUID());
	}

	private byte[] getIdAsByte(UUID uuid) {
		ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
		bb.putLong(uuid.getMostSignificantBits());
		bb.putLong(uuid.getLeastSignificantBits());
		return bb.array();
	}
}
