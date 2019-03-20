package com.wufuqiang.kafka;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * @author wufuqiang
 * @date 2019/2/15/015 - 15:06
 **/
public class CustomPartitioner implements Partitioner {

    public int partition(String s, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
        return 0;
    }

    public void close() {

    }

    public void configure(Map<String, ?> map) {

    }
}
