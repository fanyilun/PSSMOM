package fyl.middleware.mom.broker;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

/**
 * 创建唯一ID 待修改
 */
public class MessageCenter {
	/*
	 * @param idSet 用于判断产生的消息id是否已经存在 保证ID的唯一
	 */
	public static Set<String> idSet = new HashSet<String>();

	/*
	 * 产生一个八位的消息ID 范围在10000000~90000000
	 */
	public static String generateId() {
		Random random = new Random();
		String result = Integer.toString(random.nextInt(90000000) + 10000000);
		while (idSet.contains(result)) {
			result = Integer.toString(random.nextInt(90000000) + 10000000);
		}
		return result;
	}

	public static void main(String[] args) {
		System.out.println(generateId());
	}

}
