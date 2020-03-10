package com.chm.flink.cep;

import com.chm.flink.Source_Sink.DataSource5;
import com.chm.flink.pojo.Event;
import com.chm.flink.pojo.SubEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class IterativeCondition_TEST {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //DataStream<Event> ds = env.addSource(new DataSource5());
        DataStream<String> ds1 = env.socketTextStream("47.52.108.102", 9000);
        SingleOutputStreamOperator<Event> ds2 = ds1.map(
                new MapFunction<String, Event>() {
                    @Override
                    public Event map(String s) throws Exception {
                        String[] split = s.split(",");
                        Event event = new Event(split[0], Integer.valueOf(split[1]), split[2]);
                        return event;
                    }
                }
        );

        //模式
        Pattern<Event, Event> pattern = Pattern.<Event>begin("begin")
                .where(new IterativeCondition<Event>() {
                    @Override
                    public boolean filter(Event event, Context context) throws Exception {
                        return event.getType().equals("B");
                    }
                })
                .next("middle")
                //Pattern<Event, SubEvent> pattern = Pattern.<Event>begin("middle")
                .subtype(Event.class)
                .oneOrMore()
                .where(new IterativeCondition<Event>() {
                    @Override
                    public boolean filter(Event value, Context ctx) throws Exception {
                        if (value.getName().startsWith("A")) {
                            double sum = value.getPrice();
                            Iterator middle = ctx.getEventsForPattern("middle").iterator();
                            int i = 0;
                            while (middle.hasNext()) {
                                Event event = (Event) middle.next();
                                System.out.println("midddle    " + event + "  " + i++);
                                sum += event.getPrice();
                            }
                            return Double.compare(sum, 4.0) < 0;
                        }
                        return false;

                    }
                }).within(Time.minutes(1));

        PatternStream<Event> ps = CEP.pattern(ds2, pattern);

        SingleOutputStreamOperator<Event> result = ps.select(
                (Map<String, List<Event>> map) -> {
                    List<Event> middle = map.get("middle");
                    Iterator<Event> iterator = middle.iterator();
                    Event event = null;
                    while (iterator.hasNext()) {
                        event = (Event) iterator.next();
                        System.out.println("result  " + event);
                    }
                    return event;
                });

        result.print();

        env.execute("sd");
    }
}
