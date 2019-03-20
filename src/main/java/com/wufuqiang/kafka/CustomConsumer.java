package com.wufuqiang.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * @author wufuqiang
 * @date 2019/2/15/015 - 14:46
 **/
public class CustomConsumer {
    public static void main(String [] args){
        Properties props = new Properties() ;
        props.put("bootstrap.servers","10-255-0-242:9092,10-255-0-139:9092,10-255-0-197:9092") ;
        props.put("group.id","g1") ;
        props.put("enable.auto.commit","true") ;
        props.put("auto.commit.interval.ms","1000") ;
        props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer") ;
        props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer") ;
        final KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(props) ;
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable(){

            public void run() {
                if(consumer != null){
                    consumer.close() ;
                    System.out.println("wufuqiang") ;
                }
            }
        }));
        consumer.subscribe(Arrays.asList("test1")) ;
        while(true){
            ConsumerRecords<String,String> records = consumer.poll(100) ;
            for(ConsumerRecord<String,String> record : records){
                System.out.printf("offset= %d , key = %s ,value = %s \n",record.offset(),record.key(),record.value());
            }
        }



    }
}
