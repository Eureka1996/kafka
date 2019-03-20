package com.wufuqiang.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @ author wufuqiang
 * @ date 2019/2/15/015 - 14:03
 **/
public class CustomProducer {
    public static void main(String [] args){
//        配置生产者属性
        Properties props = new Properties() ;
//        配置kafka集群节点的地址，可以是多个
        props.put("bootstrap.servers","10-255-0-242:9092,10-255-0-139:9092,10-255-0-197:9092") ;
//        配置发送的消息是否等待应答
        props.put("acks","all") ;
//        配置消息发送失败的重试
        props.put("retries",0) ;
//        批量处理数据的大小：16KB
        props.put("batch.size",16384) ;
//        设置批量处理数据的延迟，单位：ms
        props.put("linger.ms",1) ;
//        设置内存缓冲区的大小
        props.put("buffer.memory",33554432) ;
//        key的序列化
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer") ;
//        value的序列化
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer") ;

//        为Producer注册拦截器
        List<String> inList = new ArrayList<String>() ;
        inList.add("com.wufuqiang.kafka.interceptor.TimeInterceptor") ;
        inList.add("com.wufuqiang.kafka.interceptor.CounterInterceptor") ;
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,inList) ;

        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(props) ;

        for(int i = 0 ; i < 50 ; i ++){
            producer.send(new ProducerRecord<String, String>("test1","maoyujiao-"+i)) ;
        }
        producer.close() ;

    }
}
