package com.alibaba.middleware.race.jstorm.test;

import com.alibaba.middleware.race.model.PaymentMessage;

/**
 * 类说明
 * @Author yangliguang
 * 2016年7月8日上午10:24:43
 */
public class TestDoPayTopic {
	public static void main(String[] args) {
		//假设这是付款时间
        Long millisTime = System.currentTimeMillis();
        //由于整分时间戳是10位数，所以需要转换成整分时间戳
        Long minuteTime = (millisTime / 1000 / 60) * 60;
        System.out.println(millisTime);
        System.out.println(minuteTime);
	}
}
