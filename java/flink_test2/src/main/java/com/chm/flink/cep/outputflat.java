package com.chm.flink.cep;

import com.chm.flink.Source_Sink.DataSource6;
import com.chm.flink.pojo.LoginEvent;
import com.chm.flink.pojo.LoginWarning;
import com.chm.flink.pojo.OutTimeEvent;
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

public class outputflat {
    public static void main(String[] args) throws  Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<String> ds = env.socketTextStream("47.52.108.102", 9000);
        SingleOutputStreamOperator<LoginEvent> loginEventStream = ds.map(
                new MapFunction<String, LoginEvent>() {
                    @Override
                    public LoginEvent map(String s) throws Exception {
                        String[] split = s.split(" ");
                        LoginEvent event = new LoginEvent(split[0], System.currentTimeMillis()+"", split[2]);
                        return event;
                    }
                }
        );

        Pattern<LoginEvent, LoginEvent> loginFailPattern = Pattern.<LoginEvent>
                begin("begin").next("one")
                .where(new IterativeCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent, Context context) throws Exception {
                        //System.out.println(loginEvent.toString());
                        return loginEvent.getType().equals("a");
                    }
                }).next("next")
                .where(new IterativeCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent, Context context) throws Exception {
                        return loginEvent.getType().equals("a");
                    }
                }).within(Time.seconds(10));


        // 登录失败事件模式定义好了之后，我们就可以把它应用到数据流当中了。
        // 因为我们是针对用户这个维度进行监听的，所以我们需要对用户进行分组，以便可以锁定用户IP。
        PatternStream<LoginEvent> patternStream = CEP.pattern(
                loginEventStream,
                loginFailPattern);

        OutputTag<OutTimeEvent> outputTag = new OutputTag<OutTimeEvent>("side-output"){};
        //输出多条
        SingleOutputStreamOperator singleOutputStreamOperator = patternStream.flatSelect(
                outputTag,
                //超时数据处理
                new PatternFlatTimeoutFunction<LoginEvent, OutTimeEvent>() {
                    @Override
                    public void timeout(Map<String, List<LoginEvent>> map, long l, Collector<OutTimeEvent> out) throws Exception {
                        List<LoginEvent> first = map.get("begin");
                        for (LoginEvent event : first) {
                            out.collect(new OutTimeEvent(event.getUserId(), event.getType(),event.getTimestamp()));
                        }
                    }
                },
                //主流程数据处理
                new PatternFlatSelectFunction<LoginEvent, LoginWarning>() {
                    @Override
                    public void flatSelect(Map<String, List<LoginEvent>> map, Collector<LoginWarning> out) throws Exception {
                        List<LoginEvent> first = map.get("one");
                        List<LoginEvent> second = map.get("next");

                        for (LoginEvent event : first) {
                            out.collect(new LoginWarning(event.getUserId(), event.getType(), event.getTimestamp()));
                        }
                        for (LoginEvent event : second) {
                            out.collect(new LoginWarning(event.getUserId(), event.getType(), event.getTimestamp()));
                        }
                        out.collect(new LoginWarning("******************************************************","",""));

                    }
                }
        );
        //获得超时数据并打印
        singleOutputStreamOperator.getSideOutput(outputTag).print();

        singleOutputStreamOperator.print();

        env.execute("output");

    }
}
