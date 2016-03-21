package fyl.middleware.mom.broker;

import java.io.FileInputStream;
import java.util.Properties;

/**
 * 记录用户配置属性
 * @author yilun.fyl
 *
 */
public class ServerConfig {

	/**
	 * Broker开放的端口号
	 */
	private int PORT = 9999;
	
	/**
	 * 合并刷盘的数量
	 */
	private int FSYNC_COMMIT_COUNT = 15;//磁盘越慢、CPU负荷越大，该值就越大
	
	/**
	 * 重发消息刷新频率（也是最小重发时间）
	 * 单位：秒
	 */
	private int RESEND_INTERVAL = 10;
	
	/**
	 * 最大重发次数
	 */
	private int MAX_RESEND_COUNT = 10;
	
	/**
	 * 消息最大长度（字节）
	 * 由于消息是定长存储的，因此值越小越好
	 */
	private int MSG_FIXED_LENGTH = 200;
	
	/**
	 * 每个文件存储的消息数量
	 */
	private int MSG_NUM_PER_FILE = 4096;
	
	/**
	 * 文件扩容临界值
	 */
	private int FLATE_THRESHOLD = (int) (MSG_NUM_PER_FILE * 0.2);

	/**
	 * 从配置文件中读取
	 */
	public void init(){
		Properties pro = new Properties();
		try {
			FileInputStream in = new FileInputStream("broker.properties");
			pro.load(in);
			in.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		String tmp = null ;
		if((tmp=pro.getProperty("port"))!=null){
			PORT = Integer.parseInt(tmp);
		}
		if((tmp=pro.getProperty("fsync_commit_count"))!=null){
			FSYNC_COMMIT_COUNT = Integer.parseInt(tmp);
		}
		if((tmp=pro.getProperty("resend_interval"))!=null){
			RESEND_INTERVAL = Integer.parseInt(tmp);
		}
		if((tmp=pro.getProperty("max_resend_count"))!=null){
			MAX_RESEND_COUNT = Integer.parseInt(tmp);
		}
		if((tmp=pro.getProperty("msg_fixed_length"))!=null){
			MSG_FIXED_LENGTH = Integer.parseInt(tmp);
		}
		if((tmp=pro.getProperty("msg_num_per_file"))!=null){
			MSG_NUM_PER_FILE = Integer.parseInt(tmp);
			FLATE_THRESHOLD = (int) (MSG_NUM_PER_FILE * 0.2);
		}
	}
	
	public int getPORT() {
		return PORT;
	}

	public int getFSYNC_COMMIT_COUNT() {
		return FSYNC_COMMIT_COUNT;
	}

	public int getRESEND_INTERVAL() {
		return RESEND_INTERVAL;
	}

	public int getMAX_RESEND_COUNT() {
		return MAX_RESEND_COUNT;
	}

	public int getMSG_FIXED_LENGTH() {
		return MSG_FIXED_LENGTH;
	}

	public int getMSG_NUM_PER_FILE() {
		return MSG_NUM_PER_FILE;
	}

	public int getFLATE_THRESHOLD() {
		return FLATE_THRESHOLD;
	}
	
}
