package com.young.utils;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 类说明
 * @Author yangliguang
 * 2016年7月9日下午12:54:29
 */
public class TimeUtil {
	/**
	 * yangliguang
	 * 时间戳转毫秒
	 */
	public static String timeStamp2Date(Long millSeconds){
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS");
		return sdf.format(millSeconds);
	}
	/**
     * 时间戳转换成日期格式字符串
     * @param seconds 精确到秒的字符串
     * @param formatStr
     * @return
     */
	public static String timeStamp2Date(String seconds,String format) {
		if(seconds == null || seconds.isEmpty() || seconds.equals("null")){
			return "";
		}
		if(format == null || format.isEmpty()) format = "yyyy-MM-dd HH:mm:ss";
		SimpleDateFormat sdf = new SimpleDateFormat(format);
		return sdf.format(new Date(Long.valueOf(seconds+"000")));
	}
	/**
	 * 日期格式字符串转换成时间戳
	 * @param date 字符串日期
	 * @param format 如：yyyy-MM-dd HH:mm:ss
	 * @return
	 */
	public static String date2TimeStamp(String date_str,String format){
		try {
			SimpleDateFormat sdf = new SimpleDateFormat(format);
			return String.valueOf(sdf.parse(date_str).getTime()/1000);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return "";
	}
	
	/**
	 * 取得当前时间戳（精确到秒）
	 * @return
	 */
	public static String timeStamp(){
		long time = System.currentTimeMillis();
		String t = String.valueOf(time/1000);
		return t;
	}
	
	//  输出结果：
	//	timeStamp=1417792627
	//	date=2014-12-05 23:17:07
	//	1417792627
	public static void main(String[] args) {
		String timeStamp = timeStamp();
		System.out.println("timeStamp="+timeStamp);
		
		String date = timeStamp2Date(timeStamp, "yyyy-MM-dd HH:mm:ss");
		System.out.println("date="+date);
		
		String timeStamp2 = date2TimeStamp(date, "yyyy-MM-dd HH:mm:ss");
		System.out.println(timeStamp2);
		
		System.out.println(timeStamp2Date(System.currentTimeMillis()));
	}
}