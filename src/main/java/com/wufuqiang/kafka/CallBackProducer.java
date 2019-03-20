package com.wufuqiang.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

/**
 * @author wufuqiang
 * @date 2019/2/15/015 - 14:31
 **/
public class CallBackProducer {
    public static void main(String [] args){
        Properties props = new Properties() ;
        props.put("bootstrap.servers","node1:9092,node2:9092,node3:9092") ;
        props.put("acks","all") ;
        props.put("retries",0) ;
        props.put("batch.size",16384) ;
        props.put("linger.ms",1) ;
        props.put("buffer.memory",33554432) ;
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer") ;
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer") ;
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(props) ;
        for(int i = 0 ; i < 50 ; i++){

            producer.send(new ProducerRecord<String, String>("test1", "myj-" + i), new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(recordMetadata != null){
                        System.out.println(recordMetadata.partition() + "-----" + recordMetadata.offset());
                    }
                }
            }) ;
        }
        producer.close() ;
    }
}
