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

import java.util.List;
import java.util.Map;

public class cepDemo_where {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> stream = env.socketTextStream("47.52.108.102", 9000);
        SingleOutputStreamOperator<LoginEvent> ds1 = stream.map(
                new MapFunction<String, LoginEvent>() {
                    @Override
                    public LoginEvent map(String ss) throws Exception {
                        String[] s = ss.split(" ");
                        LoginEvent event = new LoginEvent(s[0], s[1], s[2]);
                        return event;
                    }
                }
        );


        //  模式 严格的满足条件  当且仅当数据为a,b时，模式才会被命中。
        //  如果数据为a,c,b，由于a的后面跟了c，所以a会被直接丢弃，模式不会命中
        /*Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>
                begin("one")
                .where(new IterativeCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent, Context context) throws Exception {
                        return loginEvent.getType().equals("a");
                    }
                }).next("two")
                .where(new IterativeCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent, Context context) throws Exception {
                        return loginEvent.getType().equals("b");
                    }
                }).within(Time.seconds(60));*/

        //  松散的满足条件 当且仅当数据为a,b或者为a,c,b，，模式均被命中，中间的c会被忽略掉。
        /*Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>
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
                }).within(Time.seconds(60));*/


        //  非确定的松散满足条件  当且仅当数据为a,c,b,b时，对于followedBy模式而言命中的为{a,b}，
        //  对于followedByAny而言会有两次命中{a,b},{a,b}
        /*Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>
                begin("one")
                .where(new IterativeCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent, Context context) throws Exception {
                        return loginEvent.getType().equals("a");
                    }
                }).followedByAny("two")
                .where(new IterativeCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent, Context context) throws Exception {
                        return loginEvent.getType().equals("b");
                    }
                }).within(Time.seconds(60));
*/

        //  后面的模式不命中（严格/非严格）  忽略模式
       /* Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>
                begin("one", AfterMatchSkipStrategy.noSkip()) //忽略模式
                .where(new IterativeCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent, Context context) throws Exception {
                        return loginEvent.getType().equals("a");
                    }
                }).notNext("two")
                .where(new IterativeCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent, Context context) throws Exception {
                        return loginEvent.getType().equals("b");
                    }
                }).within(Time.seconds(60));*/


        // 组合模式 begin
        /*Pattern<LoginEvent, LoginEvent> pattern = Pattern.
                begin(
                Pattern.<LoginEvent>begin("one").where(
                        new IterativeCondition<LoginEvent>() {
                            @Override
                            public boolean filter(LoginEvent loginEvent, Context<LoginEvent> context) throws Exception {
                                return loginEvent.getType().equals("a");
                            }
                        }
                ).followedBy("two").where(
                        new IterativeCondition<LoginEvent>() {
                            @Override
                            public boolean filter(LoginEvent loginEvent, Context context) throws Exception {
                                return loginEvent.getType().equals("b");
                            }
                        }
                )
        );*/

        Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>
                begin("one")
                .where(new IterativeCondition<LoginEvent>() {
                           @Override
                           public boolean filter(LoginEvent loginEvent, Context context) throws Exception {
                               System.out.println("one      "+ loginEvent);
                               return loginEvent.getType().equals("a");
                           }
                       }
                ).next(
                        Pattern.<LoginEvent>begin("two_one").where(
                                new IterativeCondition<LoginEvent>() {
                                    @Override
                                    public boolean filter(LoginEvent loginEvent, Context<LoginEvent> context) throws Exception {
                                        System.out.println("two_one  " + loginEvent);
                                        return loginEvent.getType().equals("a");
                                    }
                                }
                        ).followedBy("two_two").where(
                                new IterativeCondition<LoginEvent>() {
                                    @Override
                                    public boolean filter(LoginEvent loginEvent, Context context) throws Exception {
                                        System.out.println("two_two  " + loginEvent);
                                        return loginEvent.getType().equals("b");
                                    }
                                }
                        )
                ).times(2);


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
                        List<LoginEvent> two = map.get("two_two");
                        for (LoginEvent event : two) {
                            out.collect(new LoginWarning(event.getUserId(), event.getType(), event.getTimestamp()));
                        }
                    }
                });
        streams.print();


        env.execute("cep");

    }

}
