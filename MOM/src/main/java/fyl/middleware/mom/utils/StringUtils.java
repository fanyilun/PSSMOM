package fyl.middleware.mom.utils;

/**
 * 字符串工具类
 * @author yilun.fyl
 */
public class StringUtils {

    public static boolean isBlank(String t){
        String s=t.replaceAll(" ","");
        if(s==null || s.equals("") || s.equals(" "))
            return  true;
        return false;
    }

}
