package com.chm.flink.wordCount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @ClassName:  SocketWindowWordCount
 * @Description:TODO(flink单词个数统计)
 * @author: Jimu
 * @email:  maker2win@163.com
 * @date:   2019年3月19日 下午3:18:03
 *
 * @Copyright: 2019 www.maker-win.net Inc. All rights reserved.
 *
 */
public class SocketWindowWordCount {
    public static void main(String[] args) throws Exception{
        //连接端口号
        final int port;
        try {
            //final ParameterTool params = ParameterTool.fromArgs(args);
            port = 9000;
        } catch (Exception e) {
            System.out.println("No port specified. Please run 'SocketWindowWordCount --port <port>'");
            return ;
        }
        //获取执行环节
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(3000);
        //获取连接socket输入数据
        DataStream<String> text = env.socketTextStream("47.52.108.102", port);
       
        //解析数据、对数据进行分组、窗口函数和统计个数

        DataStream<WordWithCount> windowCounts =text.flatMap(new FlatMapFunction<String, WordWithCount>() {
            private static final long serialVersionUID = 6800597108091365154L;
            public void flatMap(String value, Collector<WordWithCount> out) throws Exception {
                for(String word:value.split("")) {
                    out.collect(new WordWithCount(word, 1));
                }
            }
        })
                .keyBy("word")
                .timeWindow(Time.seconds(5), Time.seconds(1))
                .reduce(new ReduceFunction<WordWithCount>() {
                    public WordWithCount reduce(WordWithCount value1, WordWithCount value2) throws Exception {
                        return new WordWithCount(value1.word,value1.count+value2.count);
                    }
                });
        //windowCounts.writeAsText("C:\\Users\\SSAAS\\Desktop\\test.txt");
        windowCounts.print();
        env.execute("Socket Window WordCount");
    }

}
