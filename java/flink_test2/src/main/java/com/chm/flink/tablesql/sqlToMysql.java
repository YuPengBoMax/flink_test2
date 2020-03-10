package com.chm.flink.tablesql;

import com.chm.flink.Source_Sink.DataSource4;
import com.chm.flink.mysql.JdbcWriter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSink;
import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSinkBuilder;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

public class sqlToMysql {

    public static void main(String[] args) throws Exception {

        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        StreamExecutionEnvironment fsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(fsEnv, fsSettings);

        //datasource
        DataStreamSource<WC> order = fsEnv.addSource(new DataSource4());
        // convert DataStream to Table
        Table table = tableEnv.fromDataStream(order,"id, word, frequency");
        //从数据源读取数据转为数据流表
        Table result = tableEnv.sqlQuery("select * from " + table);


        //********** 第一种插入方式*************
        /*String[] fieldNames = {"id", "word", "frequency"};
        TypeInformation[] fieldTypes = {Types.INT, Types.STRING, Types.LONG};
        // create a TableSink
        //TableSink sink = new CsvTableSink("C:\\Users\\SSAAS\\Desktop\\test", "|");
        JDBCAppendTableSink jdbcAppendTableSink = new JDBCAppendTableSinkBuilder()
                .setDBUrl("jdbc:mysql://localhost:3306/flink_db?useSSL=false&useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC")
                .setDrivername("com.mysql.cj.jdbc.Driver")
                .setUsername("root")
                .setPassword("123456")
                .setBatchSize(5)  //积攒到此数量后插入数据库
                .setQuery("insert INTO wc(id, word, frequency) values(?,?,?)")
                .setParameterTypes(fieldTypes)
                .build();

        // register the TableSink with a specific schema
        tableEnv.registerTableSink("CsvSinkTable", fieldNames, fieldTypes, jdbcAppendTableSink);
        //tableEnv.registerTableSink("CsvSinkTable", jdbcAppendTableSink);

        // emit the result Table to the registered TableSink
        //将数据插入指定表
        result.insertInto("CsvSinkTable");*/


        //**********第二种插入方式********
        //table 转换为 stream
        DataStream<WC> ds = tableEnv.toAppendStream(result, WC.class);
        SingleOutputStreamOperator<Tuple3<Integer, String, Long>> soso = ds.map(new RichMapFunction<WC, Tuple3<Integer, String, Long>>() {
            @Override
            public Tuple3<Integer, String, Long> map(WC wc) throws Exception {
                return Tuple3.of(wc.id, wc.word, wc.frequency);
            }
        });
        soso.addSink(new JdbcWriter());

        tableEnv.execute("tableToMysql");
    }

    public static class WC {
        public String word;
        public Long frequency;
        public Integer id;

        // public constructor to make it a Flink POJO
        public WC() {
        }

        public WC(int id , String word, long frequency) {
            this.id = id;
            this.word = word;
            this.frequency = frequency;
        }

        @Override
        public String toString() {
            return "WC " + word + " " + frequency + " " + id;
        }
    }
}
