package fyl.middleware.mom.utils;

import java.util.Collection;

/**
 * Colleciton工具类
 * @author yilun.fyl
 * 
 */
public class CollectionUtils {

	public static <E> boolean isEmpty(Collection<E> collection) {
		return collection == null || collection.size() == 0;
	}
}
