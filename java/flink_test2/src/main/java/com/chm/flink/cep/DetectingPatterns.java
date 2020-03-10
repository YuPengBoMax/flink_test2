package com.chm.flink.cep;

import com.chm.flink.Source_Sink.DataSource6;
import com.chm.flink.pojo.LoginEvent;
import com.chm.flink.pojo.LoginWarning;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.EventComparator;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

public class DetectingPatterns {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<LoginEvent> loginEventStream = env.addSource(new DataSource6());

        Pattern<LoginEvent, LoginEvent> loginFailPattern = Pattern.<LoginEvent>
                begin("one")
                .where(new IterativeCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent, Context context) throws Exception {
                        return loginEvent.getType().equals("a");
                    }
                }).followedBy("two")
                .where(new IterativeCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent, Context context) throws Exception {
                        return loginEvent.getType().equals("b");
                    }
                }).within(Time.seconds(1));

        EventComparator<LoginEvent> comparator = new EventComparator<LoginEvent>() {
            @Override
            public int compare(LoginEvent o1, LoginEvent o2) {
                return o1.getType().compareTo(o2.getTimestamp());
            }
        };

        PatternStream<LoginEvent> pstream = CEP.pattern(
                //loginEventStream.keyBy(LoginEvent::getUserId),  //按用户分组
                loginEventStream,
                loginFailPattern,
                comparator);


        DataStream<LoginWarning> streams = pstream.flatSelect(
                new PatternFlatSelectFunction<LoginEvent, LoginWarning>() {
                    @Override
                    public void flatSelect(Map<String, List<LoginEvent>> map, Collector<LoginWarning> out) throws Exception {
                        List<LoginEvent> one = map.get("one");
                        List<LoginEvent> two = map.get("two");

                        for (LoginEvent event : one) {
                            out.collect(new LoginWarning(event.getUserId(), event.getType(), event.getTimestamp()));
                        }
                        for (LoginEvent event : two) {
                            out.collect(new LoginWarning(event.getUserId(), event.getType(), event.getTimestamp()));
                        }
                        out.collect(new LoginWarning("*", "*", "*********************************"));
                    }
                });

        //可以将告警事件直接打印出来
        streams.print();


        env.execute("cep");

    }
}
