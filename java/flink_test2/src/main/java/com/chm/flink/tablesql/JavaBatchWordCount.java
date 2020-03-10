package com.chm.flink.tablesql;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.types.Row;

public class JavaBatchWordCount {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

        //String path1 = JavaBatchWordCount.class.getClassLoader().getResource("words.txt").getPath();
        String path2 = "C:\\Users\\SSAAS\\Desktop\\a.txt";
        /*ConnectTableDescriptor ctd = tEnv.connect(new FileSystem().path(path2))
                //.withFormat(new FormatDescriptor().toProperties())
                .withFormat(new OldCsv().field("count", Types.STRING).lineDelimiter("\n"))
                .withSchema(new Schema().field("word", Types.STRING));
        ctd.registerTableSource("fileSource");*/

        DataSource<String> stringDataSource = env.readTextFile(path2);

        MapOperator<String, Tuple2<String, String>> mo = stringDataSource.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String s) throws Exception {
                return new Tuple2<>(s.split(" ")[0], s.split(" ")[1]);
            }
        });
        tEnv.registerDataSet("fileSource2",mo,"one, two");

        TableSource ts = new CsvTableSource(path2,new String[]{"one","two"},new TypeInformation[]{Types.STRING,Types.STRING}," ","\n",new Character('#'),false,"#",true);
        tEnv.registerTableSource("fileSource",ts);

        Table result = tEnv.scan("fileSource")
                //.groupBy("two")
                //.select("word, count(1) as count");
                .select("*");

        //tEnv.sqlUpdate("insert into fileSource +"values('qwe','adsaa')");

        Table result1 = tEnv.sqlQuery("select * from fileSource2");
        Table result2 = tEnv.sqlQuery("select * from " + result);
        tEnv.toDataSet(result2, Row.class).print();

        //result1.insertInto("fileSource","a","d");
        result1.renameColumns("one as sc, two as cs");
        result1.printSchema();
        DataSet<Result> resultDataSet = tEnv.toDataSet(result1, Result.class);
        //resultDataSet.print();

       /* TableSink tss = new CsvTableSink(path2," ",3, WriteMode.OVERWRITE);
        tEnv.registerTableSink("fileSource",tss);*/
    }


    public static class Result {
        public String one;
        public String two;

        public Result() {}
        @Override
        public String toString(){
            return this.one + " " +this.two;
        }
    }
}
