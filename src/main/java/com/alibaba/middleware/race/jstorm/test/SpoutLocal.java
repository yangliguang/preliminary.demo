package com.alibaba.middleware.race.jstorm.test;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.esotericsoftware.minlog.Log;
import com.redis.RedisClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Random;


/**
 * 类说明
 * @Author yangliguang
 * 2016年7月6日上午11:25:15
 */
//public class SpoutLocal implements IRichSpout{
public class SpoutLocal extends BaseRichSpout   
implements MessageListenerConcurrently{  
    private static Logger LOG = LoggerFactory.getLogger(SpoutLocal.class);
    SpoutOutputCollector _collector;
    Random _rand;
    long sendingCount;
    long startTime;
    boolean isStatEnable;
    int sendNumPerNexttuple;
    
    //add by Young
//    TairOperatorImpl tairOperator = new TairOperatorImpl(RaceConfig.TairConfigServer, RaceConfig.TairSalveConfigServer,
//            RaceConfig.TairGroup, RaceConfig.TairNamespace); 
    
    //begin by Young
    private transient DefaultMQPushConsumer consumer; 
    RedisClient redis;
    //end by Young

    private static final String[] CHOICES = {"marry had a little lamb whos fleese was white as snow",
            "and every where that marry went the lamb was sure to go",
            "one two three four five six seven eight nine ten",
            "this is a test of the emergency broadcast system this is only a test",
            "peter piper picked a peck of pickeled peppers"};

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
//    	System.out.println("test");
//        _collector = collector;
//        _rand = new Random();
//        sendingCount = 0;
//        startTime = System.currentTimeMillis();
//        sendNumPerNexttuple = JStormUtils.parseInt(conf.get("send.num.each.time"), 1);
//        isStatEnable = JStormUtils.parseBoolean(conf.get("is.stat.enable"), false);
    	
    	LOG.error("Young：初始化消费者");
    	redis = new RedisClient();
    	LOG.error("Young:初始化redis!!!");
    	consumer = new DefaultMQPushConsumer(RaceConfig.MetaConsumerGroup); 
    	consumer.setNamesrvAddr("127.0.0.1:9876");  
    	
    	try {  
            consumer.subscribe(RaceConfig.MqTmallTradeTopic, "*");  
            consumer.subscribe(RaceConfig.MqTaobaoTradeTopic, "*");  
            consumer.subscribe(RaceConfig.MqPayTopic, "*");  
    	} catch (MQClientException e) {  
    		e.printStackTrace();  
    	}  
        consumer.registerMessageListener(this);  
        try {  
        	consumer.start();  
        } catch (MQClientException e) {  
        	e.printStackTrace();  
        }  

        LOG.error("Young消费者启动");  
        this._collector = collector;  
    }

    public void nextTuple() {
        int n = sendNumPerNexttuple;
        while (--n >= 0) {
            Utils.sleep(10);
            String sentence = CHOICES[_rand.nextInt(CHOICES.length)];
            _collector.emit(new Values(sentence));
        }
        updateSendTps();
    }

    public void ack(Object id) {
        // Ignored
    }

    public void fail(Object id) {
        _collector.emit(new Values(id), id);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sentence"));
    }

    private void updateSendTps() {
        if (!isStatEnable)
            return;

        sendingCount++;
        long now = System.currentTimeMillis();
        long interval = now - startTime;
        if (interval > 60 * 1000) {
            LOG.info("Sending tps of last one minute is " + (sendingCount * sendNumPerNexttuple * 1000) / interval);
            startTime = now;
            sendingCount = 0;
        }
    }

    public void close() {
        // TODO Auto-generated method stub

    }

    public void activate() {
        // TODO Auto-generated method stub

    }

    public void deactivate() {
        // TODO Auto-generated method stub

    }

    public Map<String, Object> getComponentConfiguration() {
        // TODO Auto-generated method stub
        return null;
    }

	/* (non-Javadoc)
	 * @see com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently#consumeMessage(java.util.List, com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext)
	 * add by Young
	 */
	@Override
	public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
		for (MessageExt msg : msgs) {  
            byte[] body = msg.getBody();  
            if (body.length == 2 && body[0] == 0 && body[1] == 0) {  
                  
                LOG.error("Young:Got the end signal");  
                _collector.emit("stop",new Values("stop"));  
                continue;  
            }  
            if (msg.getTopic().equals(RaceConfig.MqPayTopic)) {  
                return doPayTopic(body);  
            }else if (msg.getTopic().equals(RaceConfig.MqTaobaoTradeTopic)) {  
                putTaobaoTradeToTair(body);  
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;  
            } else if (msg.getTopic().equals(RaceConfig.MqTmallTradeTopic)) {  
                putTmallTradeToTair(body);  
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;  
            }else {  
                return ConsumeConcurrentlyStatus.RECONSUME_LATER;  
            }  
        }  
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;  
	}
	
	//begin Young
	public ConsumeConcurrentlyStatus doPayTopic(byte[] body){
		PaymentMessage pm = RaceUtils.readKryoObject(PaymentMessage.class, body);
		LOG.error("处理PAY!!");
		System.out.println(pm);
		Long millisTime = pm.getCreateTime();
		Long minuteTime = (millisTime / 1000 / 60) * 60;
		
//		String salerId = (String)tairOperator.get(pm.getOrderId());
		String salerId = redis.read(pm.getOrderId()+"");
		if(salerId.startsWith("tb")){
			String key = RaceConfig.prex_taobao + minuteTime;
			if(redis.read(key) == null)
				redis.write(key, pm.getPayAmount()+"");
			else
				redis.write(key, (Double.parseDouble(redis.read(key)) + pm.getPayAmount()) + "");
		} else if(salerId.startsWith("tm")){
			String key = RaceConfig.prex_tmall + minuteTime;
			if(redis.read(key) == null)
				redis.write(key, pm.getPayAmount()+"");
		}
		
		
//		tairOperator.write(RaceConfig.prex_tmall + minuteTime, money);
		
		return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;  
	}
	public void putTaobaoTradeToTair(byte[] body){
		LOG.error("处理淘宝订单！！");
		OrderMessage om = RaceUtils.readKryoObject(OrderMessage.class, body);
		System.out.println(om);
//		tairOperator.write(om.getOrderId(), om.getSalerId());
		redis.write(om.getOrderId()+"", om.getSalerId());
		System.out.println(redis.read(om.getOrderId()+""));
		
	}
	public void putTmallTradeToTair(byte[] body){
		LOG.error("处理天猫订单！！");
		OrderMessage om = RaceUtils.readKryoObject(OrderMessage.class, body);
		System.out.println(om);
//		tairOperator.write(om.getOrderId(), om.getSalerId());
		redis.write(om.getOrderId()+"", om.getSalerId());
		redis.read(om.getOrderId()+"");
	}
}
