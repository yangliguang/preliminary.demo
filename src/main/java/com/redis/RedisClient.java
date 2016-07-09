package com.redis;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;
import redis.clients.jedis.SortingParams;



/**
 * 类说明
 * @Author yangliguang
 * 2016年7月9日上午10:39:43
 */
public class RedisClient {

    private Jedis jedis;//非切片额客户端连接
    private JedisPool jedisPool;//非切片连接池
    private ShardedJedis shardedJedis;//切片额客户端连接
    private ShardedJedisPool shardedJedisPool;//切片连接池
    
    public RedisClient() 
    { 
        initialPool(); 
        initialShardedPool(); 
        shardedJedis = shardedJedisPool.getResource(); 
        jedis = jedisPool.getResource(); 
        
        
    } 
 
    /**
     * 初始化非切片池
     */
    private void initialPool() 
    { 
        // 池基本配置 
        JedisPoolConfig config = new JedisPoolConfig(); 
        config.setMaxActive(20); 
        config.setMaxIdle(5); 
        config.setMaxWait(1000l); 
        config.setTestOnBorrow(false); 
        
        jedisPool = new JedisPool(config,"127.0.0.1",6379);
    }
    
    /** 
     * 初始化切片池 
     */ 
    private void initialShardedPool() 
    { 
        // 池基本配置 
        JedisPoolConfig config = new JedisPoolConfig(); 
        config.setMaxActive(20); 
        config.setMaxIdle(5); 
        config.setMaxWait(1000l); 
        config.setTestOnBorrow(false); 
        // slave链接 
        List<JedisShardInfo> shards = new ArrayList<JedisShardInfo>(); 
        shards.add(new JedisShardInfo("127.0.0.1", 6379, "master")); 

        // 构造池 
        shardedJedisPool = new ShardedJedisPool(config, shards); 
    } 

    public void show() {     
        KeyOperate(); 
        StringOperate(); 
        ListOperate(); 
        SetOperate();
        SortedSetOperate();
        HashOperate(); 
        jedisPool.returnResource(jedis);
        shardedJedisPool.returnResource(shardedJedis);
    } 

      private void KeyOperate() {
         //TO-DO
      }

      private void StringOperate() {
//         。。。
      }

      private void ListOperate() {
//         。。。
      }

      private void SetOperate() {
//         。。。
      }

      private void SortedSetOperate() {
//         。。。
      }
    
      private void HashOperate() {
//         。。。
      }
      
      public boolean write(String key, String value){
    	  jedis.set(key, value);
    	  return true;
      }
      
      public boolean write(byte[] key, byte[] value){
    	  jedis.set(key, value);
    	  return true;
      }
      
      public String read(String key){
    	  return jedis.get(key);
      }
      
}