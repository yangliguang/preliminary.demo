package com.redis;

import com.young.utils.TimeUtil;

/**
 * 类说明
 * @Author yangliguang
 * 2016年7月9日上午10:47:01
 */
public class Main {
	public static void main(String[] args) {
		RedisClient rs = new RedisClient();
		System.out.println(rs.read("yang"));
		rs.write("yang", "shuai2");
		System.out.println(rs.read("yang"));
		
		System.out.println(rs.read("test") == null);
		
		System.out.println(System.currentTimeMillis());
		System.out.println(TimeUtil.date2TimeStamp("1468035121164", "yyyy-MM-dd HH:mm:ss SSS"));
	}
}
