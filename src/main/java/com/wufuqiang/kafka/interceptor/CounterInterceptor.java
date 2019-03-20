package com.wufuqiang.kafka.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @author wufuqiang
 * @date 2019/2/15/015 - 20:16
 **/
public class CounterInterceptor implements ProducerInterceptor<String,String> {
    private long successCount = 0 ;
    private long errorCount = 0 ;
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        return producerRecord;
    }

    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        if(e == null){
            successCount++ ;
        }else{
            errorCount++ ;
        }

    }

    public void close() {
        System.out.println("成功个数："+successCount+"，失败个数："+errorCount);
    }

    public void configure(Map<String, ?> map) {

    }
}
