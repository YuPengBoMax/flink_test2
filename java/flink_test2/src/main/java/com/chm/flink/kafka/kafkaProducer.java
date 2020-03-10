package com.chm.flink.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;

public class kafkaProducer {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "rh-bigdata-001:6667,rh-bigdata-002:6667,rh-bigdata-003:6667"); //服务器id
        props.setProperty("acks", "all");
        props.setProperty("retries", "0");
        props.setProperty("batch.size", "16384");
        props.setProperty("linger.ms", "1");
        props.setProperty("buffer.memory", "33554432");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());

        KafkaProducer producer = new KafkaProducer<String, String>(props);
        Random random = new Random(2);

        //指定发送任意格式的数据到kafka
        for (int i = 0; i < 100000; i++) {
            String data="";
            producer.send(new ProducerRecord<String, String>
                    ("cn_etl_cqll_createUserLog_test", data+random.nextInt(10))
            );
            Thread.sleep(1000);
        }

    }
}