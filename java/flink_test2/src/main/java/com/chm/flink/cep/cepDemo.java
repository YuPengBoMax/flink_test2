package com.chm.flink.cep;

import com.chm.flink.Source_Sink.DataSource6;
import com.chm.flink.pojo.LoginEvent;

import com.chm.flink.pojo.LoginWarning;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
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
import java.util.stream.Collectors;

/**
 *  复杂事件编程
 */
public class cepDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<LoginEvent> loginEventStream = env.addSource(new DataSource6());

        // 开始定义匹配模式,首先为第1条日志定义一个first名称，如果满足第一个where条件，
        // 则进入下一个监听事件second,如果在1秒钟之内两个模式都满足，则成为loginFail告警。
        // 示例：
        // 模式为begin("first").where(_.name='a').next("second").where(.name='b').with(Time.seconds(1))
        // 在指定时间内, 当数据为a,b时，模式会被命中。则如果数据为a,c,b，由于a的后面跟了c，所以a会被直接丢弃，模式不会命中。
        Pattern<LoginEvent, LoginEvent> loginFailPattern = Pattern.<LoginEvent>
                begin("begin")
                //begin("begin", AfterMatchSkipStrategy.noSkip())
                .where(new IterativeCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent, Context context) throws Exception {
                        //System.out.println(loginEvent.toString());
                        return loginEvent.getType().equals("B");
                    }
                }).followedBy("next")
                .where(new IterativeCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent, Context context) throws Exception {
                        return loginEvent.getType().equals("B");
                    }
                }).within(Time.seconds(1));


        // 登录失败事件模式定义好了之后，我们就可以把它应用到数据流当中了。
        // 因为我们是针对用户这个维度进行监听的，所以我们需要对用户进行分组，以便可以锁定用户IP。
        PatternStream<LoginEvent> patternStream = CEP.pattern(
                //loginEventStream.keyBy(LoginEvent::getUserId),  //按用户分组
                loginEventStream,
                loginFailPattern);



        //所匹配的到事件会以一个Map<String, List<LoginEvent>>返回,key为事件名称，value为匹配的数据列表。
        // 输出一条
        DataStream<LoginWarning> loginFailDataStream = patternStream.select(
                (Map<String, List<LoginEvent>> pattern) -> {
                    List<LoginEvent> first = pattern.get("begin");
                    LoginEvent fevent = first.iterator().next();
                    List<LoginEvent> second = pattern.get("next");
                    LoginEvent event = second.iterator().next();
                    return new LoginWarning(event.getUserId(), event.getType(), event.getTimestamp());
                });

        //可以将告警事件直接打印出来
        loginFailDataStream.print();





        env.execute("cep");

    }
}
