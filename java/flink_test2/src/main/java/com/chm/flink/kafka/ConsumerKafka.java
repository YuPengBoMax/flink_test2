package com.chm.flink.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class ConsumerKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        props.put("bootstrap.servers", "rh-bigdata-001:6667"); //服务器id
        props.put("group.id", "flink_test"); //消费者组
        props.put("enable.auto.commit", "true"); // 是否自动提交offset
        props.put("auto.commit.interval.ms", "1000"); //    自动处理的间隔时间
        //设置会话响应的时间，超过这个时间kafka可以选择放弃消费或者消费下一条消息
        props.put("session.timeout.ms", "30000");
        // key 和 value 的持久化设置
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put("auto.offset.reset", "latest"); // 消费方式
        /* earliest     当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费
           latest       当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，消费新产生的该分区下的数据
           none         topic各分区都存在已提交的offset时，从offset后开始消费；只要有一个分区不存在已提交的offset，则抛出异常   */

        FlinkKafkaConsumer<String> kafkaconsumer = new FlinkKafkaConsumer<>("cn_etl_cqll_createUserLog_test", new SimpleStringSchema(), props);

        DataStreamSource<String> ksource = sEnv.addSource(kafkaconsumer);

        ksource.print();

        sEnv.execute("kafkaconsumer");


    }
}
