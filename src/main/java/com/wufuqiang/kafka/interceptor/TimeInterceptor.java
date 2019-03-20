package com.wufuqiang.kafka.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @author wufuqiang
 * @date 2019/2/15/015 - 20:08
 **/
public class TimeInterceptor implements ProducerInterceptor<String,String> {

    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        return new ProducerRecord<String, String>(
                producerRecord.topic(),
                producerRecord.partition(),
                producerRecord.key(),
                System.currentTimeMillis() + "-" +producerRecord.value());
    }

    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {

    }

    public void close() {

    }

    public void configure(Map<String, ?> map) {

    }
}
