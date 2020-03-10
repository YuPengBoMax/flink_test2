package com.chm.flink.mysql;

import com.chm.flink.Source_Sink.DataSource3;
import com.chm.flink.Source_Sink.DataSource4;
import com.chm.flink.tablesql.sqlToMysql;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class mysqltest {
    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Tuple3<Integer, String, Long>> source = env.addSource(new JdbcReader());
        source.print();

        DataStreamSource<Tuple3<Integer,String,Long>> source1 = env.addSource(new DataSource3());
        source1.addSink(new JdbcWriter());

        env.execute("adf");

    }
}
