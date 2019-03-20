package com.wufuqiang.kafka.stream;


import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

/**
 * @ author wufuqiang
 * @ date 2019/2/15/015 - 20:47
 **/
public class LogProcessor implements Processor<byte[],byte[]> {
    private ProcessorContext context = null ;
    public void init(ProcessorContext processorContext) {
        this.context = processorContext ;
    }

    public void process(byte[] key, byte[] value) {
        String inputOri = new String(value) ;
        if(inputOri.contains(">>>")){
            inputOri = inputOri.split(">>>")[1] ;
        }
        context.forward(key,inputOri.getBytes());
    }

    public void punctuate(long l) {

    }

    public void close() {

    }

}
