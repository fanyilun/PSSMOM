package fyl.middleware.mom.data;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;

import fyl.middleware.mom.api.MessageExt;
import fyl.middleware.mom.encode.MsgExtEncoder;

/**
 * broker消息存储专用
 * @author yilun.fyl
 *
 */
public class DataHelper {

	private static File file;
	private static MappedByteBuffer mapBuf;
	private static String filePath;
	private final static String MESSAGELOG_PREFIX = "MessageLog";
	private final static int m1Size = 5000;
	private static Deque<Long> m1ValidQueue;

	private static MsgExtEncoder msgserialization;
	private static ByteBuf serialBuf;
	private static RandomAccessFile raf;
	static {
		if (System.getProperty("user.home") != null) {
			filePath = System.getProperty("user.home") + File.separator
					+ "store/";
		} else {
			filePath = "/userhome" + File.separator + "store/";
		}
		// filePath = "G:/123/";
		File rootFile = new File(filePath);
		rootFile.setWritable(true, false);
		File[] files = rootFile.listFiles();
		int currentNo = 0;
		if (files != null) {
			currentNo = files.length == 0 ? 0 : files.length;
		}
		file = new File(filePath + MESSAGELOG_PREFIX + currentNo + ".store");
		file.setWritable(true, false);
		if (!file.exists()) {
			if (!file.getParentFile().exists()) {
				file.getParentFile().mkdirs();
			}
			try {
				file.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		try {
			raf = new RandomAccessFile(file, "rw");
			m1ValidQueue = new ConcurrentLinkedDeque<Long>();
			for (int i = 0; i < m1Size; i++) {
				m1ValidQueue.add((long) i);
			}
			int mapsize = 200 * m1Size;// 100K
			FileChannel fc = raf.getChannel();
			mapBuf = fc.map(MapMode.READ_WRITE, 0, mapsize);
		} catch (Exception e) {
			e.printStackTrace();
		}

		msgserialization = new MsgExtEncoder();
		serialBuf = Unpooled.buffer(200 - 4, 200 - 4);
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
	public static void force() {
		mapBuf.force();
	}

	public static long saveMessage(MessageExt message) {
		if (mapBuf == null) {
			System.out.println("raf=null");
			return -1;
		} else {
			try {
				serialBuf.clear();
				msgserialization.encode(serialBuf, message);
				byte[] a = serialBuf.array();
				Long index = m1ValidQueue.poll();
				if (index == null) {// TODO 扩容
					System.out.println("queue is empty!!!");
					return -1;
				}
				synchronized (mapBuf) {
					// TODO 寻址
					mapBuf.position((int) (index * 200));
					mapBuf.putInt(a.length);
					mapBuf.put(a);
				}
				return index;
			} catch (Exception e) {
				System.out.println(e + "....");
				e.printStackTrace();
			}
			return -1;
		}

	}

	public static boolean deleteMessage(long index) {
		if (index < 0) {
			return false;
		}
		if (mapBuf != null) {
			try {
				// TODO 寻址
				synchronized (mapBuf) {
					mapBuf.position((int) (index * 200));
					mapBuf.putInt(0);
				}
				m1ValidQueue.add(index);
			} catch (Exception e) {
				e.printStackTrace();
				return false;
			}
		}
		return true;
	}

	public static void close() {
		try {
			raf.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
