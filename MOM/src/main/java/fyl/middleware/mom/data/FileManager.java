package fyl.middleware.mom.data;

import io.netty.buffer.Unpooled;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import fyl.middleware.mom.api.MessageExt;
import fyl.middleware.mom.api.MsgID;
import fyl.middleware.mom.broker.ServerConfig;
import fyl.middleware.mom.encode.MsgExtDecoder;

/**
 * 存储文件管理，实现动态伸缩
 * 
 * @author yilun.fyl
 * 
 */
public class FileManager {

	private static List<MappedByteBuffer> mappedList;
	/**
	 * 与上面的list一一对应
	 */
	private static List<RandomAccessFile> fileList;

	private static List<Boolean> haveNewMsg;
	private static int baseNo;
	private static int currentNo;
	private final static String MESSAGELOG_PREFIX = "MessageLog";
	private final static String DEAD_MESSAGELOG_PREFIX = "DeadMessageLog";
	private static String filePath;
	private static volatile int queueLength;
	private static MsgExtDecoder msgExtDecoder;
	private static ServerConfig serverConfig;
	private static OutputStream deadMsgOutput;

	static void init(ServerConfig serverConfig) {
		FileManager.serverConfig = serverConfig;
		if (System.getProperty("user.home") != null) {
			filePath = System.getProperty("user.home") + File.separator
					+ "store/";
		} else {
			filePath = "/userhome" + File.separator + "store/";
		}
		filePath = "G:/123/";
		File rootFile = new File(filePath);
		rootFile.setWritable(true, false);
		File[] files = rootFile.listFiles();
		if (files != null) {
			baseNo = files.length == 0 ? 0 : files.length;
		}
		msgExtDecoder = new MsgExtDecoder();
		mappedList = new ArrayList<MappedByteBuffer>();
		fileList = new ArrayList<RandomAccessFile>();
		haveNewMsg = new ArrayList<Boolean>();
		creatFile();
		try {
			deadMsgOutput = Files.newOutputStream(Paths.get(filePath + DEAD_MESSAGELOG_PREFIX+".store"));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 增加存储文件
	 * 
	 * @return 是否成功
	 */
	private static boolean creatFile() {
		File file = new File(filePath + MESSAGELOG_PREFIX + currentNo
				+ ".store");
		file.setWritable(true, false);
		if (!file.exists()) {
			if (!file.getParentFile().exists()) {
				file.getParentFile().mkdirs();
			}
			try {
				file.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
				return false;
			}
		}
		try {
			RandomAccessFile raf = new RandomAccessFile(file, "rw");
			int mapsize = serverConfig.getMSG_FIXED_LENGTH()
					* serverConfig.getMSG_NUM_PER_FILE();// 100K
			FileChannel fc = raf.getChannel();
			MappedByteBuffer mapBuf = fc.map(MapMode.READ_WRITE, 0, mapsize);
			mappedList.add(mapBuf);
			fileList.add(raf);
			haveNewMsg.add(false);
			if (currentNo < baseNo) {
				for (int i = 0; i < serverConfig.getMSG_NUM_PER_FILE(); i++) {
					DataHelper.appendToQueue((long) (currentNo
							* serverConfig.getMSG_NUM_PER_FILE() + i));
				}
			} else {
				for (int i = 0; i < serverConfig.getMSG_NUM_PER_FILE(); i++) {
					mapBuf.position(i * serverConfig.getMSG_FIXED_LENGTH());
					int a = mapBuf.getInt();
					if (a == 0) {
						DataHelper.appendToQueue((long) (currentNo
								* serverConfig.getMSG_NUM_PER_FILE() + i));
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		currentNo++;
		queueLength += serverConfig.getMSG_NUM_PER_FILE();
		return true;
	}

	public static void writeData(byte[] data, long globalIndex) {
		int fileIndex = (int) (globalIndex / serverConfig.getMSG_NUM_PER_FILE());
		MappedByteBuffer mapBuf = mappedList.get(fileIndex);
		synchronized (mapBuf) {
			mapBuf.position((int) ((globalIndex % serverConfig
					.getMSG_NUM_PER_FILE()) * serverConfig
					.getMSG_FIXED_LENGTH()));
			mapBuf.putInt(data.length);
			mapBuf.put(data);
		}
		queueLength--;
		if (queueLength == serverConfig.getFLATE_THRESHOLD()) {
			creatFile();// 扩容
		}
		haveNewMsg.set(fileIndex, true);
	}

	/**
	 * 启动时调用，从磁盘文件中取出尚未消费成功的消息
	 * 
	 * @return
	 */
	@SuppressWarnings("resource")
	public static List<MessageExt> recoverUnsendMsg() {
		List<MessageExt> list = new ArrayList<MessageExt>();
		if (baseNo == 0) {
			// 说明没有残余消息
			return list;
		}
		for (int i = 0; i < baseNo; i++) {
			try {
				InputStream input = new FileInputStream(new File(filePath
						+ MESSAGELOG_PREFIX + i + ".store"));
				DataInputStream dis = new DataInputStream(input);
				for (int j = 0; j < serverConfig.getMSG_NUM_PER_FILE(); j++) {
					if (dis.readInt() == 0) {
						dis.skip(serverConfig.getMSG_FIXED_LENGTH() - 4);// int都是4个字节处理的，与平台无关
						continue;
					}
					dis.skip(1);
					byte[] tmp = new byte[serverConfig.getMSG_FIXED_LENGTH() - 5];
					dis.read(tmp);
					list.add(msgExtDecoder.decode(Unpooled.wrappedBuffer(tmp)));
				}
			} catch (IOException e) {
				
			}
		}
		return list;
	}

	public static MessageExt readMsgFromFile(long globalIndex) {
		int fileIndex = (int) (globalIndex / serverConfig.getMSG_NUM_PER_FILE());
		MappedByteBuffer mapBuf = mappedList.get(fileIndex);
		byte[] tmp = new byte[serverConfig.getMSG_FIXED_LENGTH() - 5];
		synchronized (mapBuf) {
			mapBuf.position((int) ((globalIndex % serverConfig
					.getMSG_NUM_PER_FILE()) * serverConfig
					.getMSG_FIXED_LENGTH()) + 5);
			mapBuf.get(tmp);
		}
		return msgExtDecoder.decode(Unpooled.wrappedBuffer(tmp));
	}

	/**
	 * 为防止重复发送的ConsumeResult 覆盖掉存储的新消息 在删除之前会根据messageID验证
	 * 
	 * @param globalIndex
	 * @param msgId
	 * @return 是否成功删除
	 */
	public static boolean validateAndDelete(long globalIndex, MsgID msgId) {
		int fileIndex = (int) (globalIndex / serverConfig.getMSG_NUM_PER_FILE());
		MappedByteBuffer mapBuf = mappedList.get(fileIndex);
		byte[] tmp = new byte[16];
		int startPosition = (int) ((globalIndex % serverConfig
				.getMSG_NUM_PER_FILE()) * serverConfig.getMSG_FIXED_LENGTH());
		boolean success = false;
		synchronized (mapBuf) {
			mapBuf.position(startPosition + 5);
			mapBuf.get(tmp);
			if (Arrays.equals(tmp, msgId.getIdData())) {
				// 删除
				mapBuf.position(startPosition);
				mapBuf.putInt(0);
				success = true;
				queueLength++;
			}
		}
		return success;
	}

	public static void force() {
		for (int i = 0; i < haveNewMsg.size(); i++) {
			if (haveNewMsg.get(i)) {
				mappedList.get(i).force();
				haveNewMsg.set(i, false);
			}
		}
	}

	public static void close() {
		for (RandomAccessFile file : fileList) {
			try {
				file.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static void saveDeadMessage(byte[] data) {
		try {
			deadMsgOutput.write(data);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
