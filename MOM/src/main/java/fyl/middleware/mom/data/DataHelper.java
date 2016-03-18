package fyl.middleware.mom.data;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import fyl.middleware.mom.api.MessageExt;
import fyl.middleware.mom.encode.MsgExtEncoder;

/**
 * broker消息存储专用
 * 
 * @author yilun.fyl
 * 
 */
public class DataHelper {

	private static Queue<Long> m1ValidQueue;
	private static MsgExtEncoder msgserialization;
	private static ByteBuf serialBuf;
	private static final int MsgFixedLength = 200;

	static {
		m1ValidQueue = new ConcurrentLinkedQueue<Long>();
		msgserialization = new MsgExtEncoder();
		serialBuf = Unpooled.buffer(MsgFixedLength - 4, MsgFixedLength - 4);
		FileManager.init();
	}

	// public static Map<String,Message> loadMessage(){
	// Map<String,Message> map=new HashMap<String, Message>();
	// if(raf!=null){
	// try {
	// KryoSerialization kryoSerialization = new KryoSerialization(kyroFactory);
	// raf.seek(0);
	// int length=raf.read();
	// System.out.println(length);
	// while(length!=0&&length!=-1){
	// if(length<0){
	// //说明删除
	// byte[] bytes=new byte[-length];
	// raf.read(bytes);
	// continue;
	// }
	// byte[] bytes=new byte[length];
	// raf.read(bytes);
	// System.out.println(new String(bytes));
	// try {
	// Message m=(Message) kryoSerialization.deserialize(new
	// ByteArrayInputStream(bytes));
	// } catch (Exception e) {
	// e.printStackTrace();
	// break;
	// }
	// length=raf.read();
	// }
	// } catch (IOException e) {
	// e.printStackTrace();
	// }
	// }
	// return map;
	// }

	public static long saveMessage(MessageExt message) {
		try {
			serialBuf.clear();
			msgserialization.encode(serialBuf, message);
			byte[] a = serialBuf.array();
			Long index = m1ValidQueue.poll();
			if (index == null) {// 异常
				System.out.println("queue is empty!!!");
				return -1;
			}
			FileManager.writeData(a, index);
			return index;
		} catch (Exception e) {
			System.out.println(e + "....");
			e.printStackTrace();
		}
		return -1;

	}

	public static boolean deleteMessage(long index) {
		if (index < 0) {
			return false;
		}
		FileManager.deleteData(index);
		m1ValidQueue.add(index);
		return true;
	}

	public static void close() {
		FileManager.close();
	}

	public static void appendToQueue(Long num) {
		m1ValidQueue.add(num);
	}
}
