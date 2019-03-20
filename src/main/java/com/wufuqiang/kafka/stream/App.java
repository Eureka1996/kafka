package com.wufuqiang.kafka.stream;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;

import java.util.Properties;

/**
 * @author wufuqiang
 * @date 2019/2/15/015 - 21:19
 **/
public class App {
    public static void main(String [] args){
        String fromTopic = "test1" ;
        String toTopic = "test2" ;
        Properties props = new Properties() ;
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,"logProcessor") ;
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"node1:9092,node2:9092,node3:9092") ;

        StreamsConfig config = new StreamsConfig(props) ;
        TopologyBuilder buidler = new TopologyBuilder() ;
        buidler
                .addSource("SOURCE",fromTopic)
                .addProcessor("PROCESSOR", new ProcessorSupplier<byte[],byte[]>() {
            public Processor<byte[],byte[]> get() {
                return new LogProcessor();
            }
        },"SOURCE")
                .addSink("SINK",toTopic,"PROCESSOR") ;
        KafkaStreams streams = new KafkaStreams(buidler,config) ;
        streams.start();
    }
}
