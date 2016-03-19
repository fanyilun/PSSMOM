package fyl.middleware.mom.data;

import io.netty.buffer.Unpooled;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import fyl.middleware.mom.api.MessageExt;
import fyl.middleware.mom.api.MsgID;
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
	private final static int MsgNumPerFile = 4096;
	private static String filePath;
	private static final int MsgFixedLength = 200;
	private static final int flateThreshold = (int) (MsgNumPerFile * 0.2);
	private static volatile int queueLength;
	private static MsgExtDecoder msgExtDecoder;

	static void init() {
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
	}

	/**
	 * 增加存储文件
	 * 
	 * @return 是否成功
	 */
	private static boolean creatFile() {
		File file = new File(filePath + MESSAGELOG_PREFIX + (baseNo+currentNo)
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
			int mapsize = 200 * MsgNumPerFile;// 100K
			FileChannel fc = raf.getChannel();
			MappedByteBuffer mapBuf = fc.map(MapMode.READ_WRITE, 0, mapsize);
			mappedList.add(mapBuf);
			fileList.add(raf);
			haveNewMsg.add(false);
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		for (int i = 0; i < MsgNumPerFile; i++) {
			DataHelper.appendToQueue((long) (currentNo * MsgNumPerFile + i));
		}
		currentNo++;
		queueLength += MsgNumPerFile;
		return true;
	}

	public static void writeData(byte[] data, long globalIndex) {
		int fileIndex = (int) (globalIndex / MsgNumPerFile);
		MappedByteBuffer mapBuf = mappedList.get(fileIndex);
		synchronized (mapBuf) {
			mapBuf.position((int) ((globalIndex % MsgNumPerFile) * MsgFixedLength));
			mapBuf.putInt(data.length);
			mapBuf.put(data);
		}
		queueLength--;
		if (queueLength == flateThreshold) {
			creatFile();// 扩容
		}
		haveNewMsg.set(fileIndex, true);
	}

	public static MessageExt readMsgFromFile(long globalIndex){
		int fileIndex = (int) (globalIndex / MsgNumPerFile);
		MappedByteBuffer mapBuf = mappedList.get(fileIndex);
		byte[] tmp = new byte[MsgFixedLength-5];
		synchronized (mapBuf) {
			mapBuf.position((int) ((globalIndex % MsgNumPerFile) * MsgFixedLength)+5);
			mapBuf.get(tmp);
		}
		return msgExtDecoder.decode(Unpooled.wrappedBuffer(tmp));
	}
	
	/**
	 * 为防止重复发送的ConsumeResult 覆盖掉存储的新消息
	 * 在删除之前会根据messageID验证
	 * @param globalIndex
	 * @param msgId
	 * @return 是否成功删除
	 */
	public static boolean validateAndDelete(long globalIndex,MsgID msgId){
		int fileIndex = (int) (globalIndex / MsgNumPerFile);
		MappedByteBuffer mapBuf = mappedList.get(fileIndex);
		byte[] tmp = new byte[16];
		int startPosition = (int) ((globalIndex % MsgNumPerFile) * MsgFixedLength);
		boolean success = false;
		synchronized (mapBuf) {
			mapBuf.position(startPosition+5);
			mapBuf.get(tmp);
			if(Arrays.equals(tmp, msgId.getIdData())){
				//删除
				mapBuf.position(startPosition);
				mapBuf.putInt(0);
				success=true;
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
			}
		}
	}
}
