package com.chm.flink.cep;

import com.chm.flink.pojo.LoginEvent;
import com.chm.flink.pojo.LoginWarning;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class ignore {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> stream = env.socketTextStream("47.52.108.102", 9000);
        SingleOutputStreamOperator<LoginEvent> ds1 = stream.map(
                new MapFunction<String, LoginEvent>() {
                    @Override
                    public LoginEvent map(String ss) throws Exception {
                        String[] split = ss.split(" ");
                        LoginEvent event = new LoginEvent(split[0], split[1], split[2]);
                        return event;
                    }
                }
        );

        Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>
                //begin("one")
                //begin("one", AfterMatchSkipStrategy.noSkip())
                //begin("one", AfterMatchSkipStrategy.skipPastLastEvent())
                begin("one", AfterMatchSkipStrategy.skipToLast("two"))
                //begin("begin", AfterMatchSkipStrategy.skipToFirst("two"))
                .where(new IterativeCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent, Context context) throws Exception {
                        //System.out.println(loginEvent.toString());
                        return loginEvent.getType().equals("a");
                    }
                }).oneOrMore()
                .followedBy("two")
                .where(new IterativeCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent, Context context) throws Exception {
                        return loginEvent.getType().equals("b");
                    }
                }).within(Time.seconds(60));

        PatternStream<LoginEvent> pstream = CEP.pattern(
                ds1,
                pattern);


        //所匹配的到事件会以一个Map<String, List<LoginEvent>>返回,key为事件名称，value为匹配的数据列表。
        // 输出一条
        DataStream<LoginWarning> streams = pstream.flatSelect(
                new PatternFlatSelectFunction<LoginEvent, LoginWarning>() {
                    @Override
                    public void flatSelect(Map<String, List<LoginEvent>> map, Collector<LoginWarning> out) throws Exception {
                        List<LoginEvent> one = map.get("one");
                        List<LoginEvent> two = map.get("two");

                        if (one != null) {
                            for (LoginEvent event : one) {
                                out.collect(new LoginWarning(event.getUserId(), event.getType(), event.getTimestamp()));
                            }
                        }
                        for (LoginEvent event : two) {
                            out.collect(new LoginWarning(event.getUserId(), event.getType(), event.getTimestamp()));
                        }
                        out.collect(new LoginWarning("*","*","***********************************"));
                    }
                });
        streams.print();
        env.execute("jgnore");

    }
}
