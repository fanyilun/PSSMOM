package fyl.middleware.mom.data;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import fyl.middleware.mom.api.MessageExt;
import fyl.middleware.mom.api.MsgID;
import fyl.middleware.mom.broker.ServerConfig;
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
	/**
	 * 存储序列化用的字节缓冲区 防止高并发下的锁竞争，此处以数组的形式分摊了竞争
	 */
	private static ByteBuf[] serialBuf;

	public static void init(ServerConfig serverConfig) {
		m1ValidQueue = new ConcurrentLinkedQueue<Long>();
		msgserialization = new MsgExtEncoder();
		serialBuf = new ByteBuf[16];
		for (int i = 0; i < serialBuf.length; i++) {
			serialBuf[i] = Unpooled.buffer(
					serverConfig.getMSG_FIXED_LENGTH() - 4,
					serverConfig.getMSG_FIXED_LENGTH() - 4);
		}
		FileManager.init(serverConfig);
	}

	public static long saveMessage(MessageExt message) {
		try {
			Long index = m1ValidQueue.poll();
			if (index == null) {// 异常
				System.out.println("queue is empty!!!");
				return -1;
			}
			message.setStoreIndex(index);
			byte[] a = null;
			int n = message.getMsgId().hashCode() & 0xf; // 取模16
			synchronized (serialBuf[n]) {
				serialBuf[n].clear();
				msgserialization.encode(serialBuf[n], message);
				a = serialBuf[n].array();
			}
			FileManager.writeData(a, index);
			return index;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return -1;

	}

	public static boolean deleteMessage(long index, MsgID MsgId) {
		if (index < 0) {
			return false;
		}
		if (FileManager.validateAndDelete(index, MsgId)) {
			m1ValidQueue.add(index);
			return true;
		}
		return false;
	}

	public static void close() {
		FileManager.close();
	}

	public static void appendToQueue(Long num) {
		m1ValidQueue.add(num);
	}

	public static void saveDeadMessage(MessageExt message) {
		byte[] a = null;
		int n = message.getMsgId().hashCode() & 0xf; // 取模16
		synchronized (serialBuf[n]) {
			serialBuf[n].clear();
			msgserialization.encode(serialBuf[n], message);
			a = serialBuf[n].array();
		}
		FileManager.saveDeadMessage(a);
	}

}
