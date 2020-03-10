package com.chm.flink.cep;

import com.chm.flink.Source_Sink.DataSource6;
import com.chm.flink.pojo.Event;
import com.chm.flink.pojo.LoginEvent;
import com.chm.flink.pojo.LoginWarning;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;


public class cepDemo_time {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setParallelism(2);
        DataStream<String> ds = env.socketTextStream("47.52.108.102", 9000);
        SingleOutputStreamOperator<LoginEvent> ds1 = ds.map(
                new MapFunction<String, LoginEvent>() {
                    @Override
                    public LoginEvent map(String s) throws Exception {
                        String[] split = s.split(" ");
                        LoginEvent event = new LoginEvent(split[0], split[1], split[2]);
                        return event;
                    }
                }
        );


        //模式
        Pattern<LoginEvent, LoginEvent> loginFailPattern1 = Pattern.<LoginEvent>
                begin("begin")
                .oneOrMore();             //模式发生1次或多次
                //.times(2, 4).greedy();    //模式发生2次且重复次数越多越好
                //.times(2).optional();     //模式发生2次或者0次
                //.timesOrMore(2);          //模式发生大于等于2次
                //.times(2,4);              //模式发生2,3,4次
                //.times(2);                //模式发生2次


        PatternStream<LoginEvent> patternStreams = CEP.pattern(
                ds1,
                loginFailPattern1);


        //所匹配的到事件会以一个Map<String, List<LoginEvent>>返回,key为事件名称，value为匹配的数据列表。
        // 输出一条
        DataStream<LoginWarning> stream = patternStreams.select(
                (Map<String, List<LoginEvent>> pattern) -> {
                    List<LoginEvent> first = pattern.get("begin");
                    for (LoginEvent le : first) {
                        System.out.println(le);
                    }
                    LoginEvent event = first.iterator().next();
                    return new LoginWarning(event.getUserId(), event.getType(), event.getTimestamp());
                });
        stream.print();

        env.execute("cep");
    }
}
