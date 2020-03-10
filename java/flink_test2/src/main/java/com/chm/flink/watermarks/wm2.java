package com.chm.flink.watermarks;

import com.chm.flink.Source_Sink.DataSource1;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.*;

public class wm2 {
    public static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    //固定watermark
    static AssignerWithPunctuatedWatermarks a = new AssignerWithPunctuatedWatermarks(){
        @Override
        public long extractTimestamp(Object o, long l) {
            String s = (String) o;
            long timestamp = Long.valueOf(s.split(" ")[1]);
            long wm = System.currentTimeMillis() - maxTimeLag;
            String wmtime = format.format(new Date(wm));
            System.out.println(format.format(timestamp) + "(" + timestamp + ")" + " | "  + " | " + wmtime + "(" + wm + ")");
            return Long.valueOf(timestamp);
        }
        //最大可延迟时间
        private final long maxTimeLag = 5000;
        public Watermark watermark;
        @Nullable
        @Override
        public Watermark checkAndGetNextWatermark(Object o, long l) {
            watermark = new Watermark(System.currentTimeMillis() - maxTimeLag);
            return watermark;
        }
    };
    static AssignerWithPeriodicWatermarks assigner_1 = new AssignerWithPeriodicWatermarks() {

        private long currentMaxTimestamp = 0L;

        public long extractTimestamp(Object logStr, long lon) {
            String s = (String) logStr;
            long timestamp = Long.valueOf(s.split(" ")[1]);
            long wm = watermark.getTimestamp();
            String wmtime = format.format(new Date(wm));
            System.out.println(format.format(timestamp) + "(" + timestamp + ")" + " | " + currentMaxTimestamp + " | " + wmtime + "(" + wm + ")");
            return Long.valueOf(timestamp);
        }

        //最大可延迟时间
        private final long maxTimeLag = 5000;
        public Watermark watermark;

        @Nullable
        public Watermark getCurrentWatermark() {
            watermark = new Watermark(System.currentTimeMillis() - maxTimeLag);
            return watermark;
        }
    };
    //周期性watermark
    static AssignerWithPeriodicWatermarks assigner_2 = new AssignerWithPeriodicWatermarks() {

        private long currentMaxTimestamp = 0L;

        public long extractTimestamp(Object logStr, long lon) {
            String s = (String) logStr;
            long timestamp = Long.valueOf(s.split(" ")[1]);
            currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
            long wm = currentMaxTimestamp - maxTimeLag;
            String wmtime = format.format(new Date(wm));
            System.out.println(format.format(timestamp) + "(" + timestamp + ")" + " | " + currentMaxTimestamp + " | " + wmtime + "(" + wm + ")");
            return Long.valueOf(timestamp);
        }

        //最大可延迟时间
        private final long maxTimeLag = 4000;
        public Watermark watermark;

        @Nullable
        public Watermark getCurrentWatermark() {
            watermark = new Watermark(currentMaxTimestamp - maxTimeLag);
            return watermark;
        }
    };


    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(3000);
        //开启 EventTime 时间模型
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //DataStream<String> ds = env.socketTextStream("47.52.108.102", 9000).assignTimestampsAndWatermarks(assigner_2);
        DataStream<String> ds = env.addSource(new DataSource1()).assignTimestampsAndWatermarks(a);
        SingleOutputStreamOperator<List> ds1 = ds.map(new MapFunction<String, Tuple2<String, Long>>() {
            public Tuple2 map(String val) throws Exception {
                return Tuple2.of(val.split(" ")[0], Long.valueOf(val.split(" ")[1]));
            }
        }).keyBy(new KeySelector<Tuple2<String, Long>, String>() {
            public String getKey(Tuple2<String, Long> val) throws Exception {
                return val.f0;
            }
        }).window(TumblingEventTimeWindows.of(Time.seconds(10)))
                //.timeWindow(Time.seconds(10), Time.seconds(5))
                .apply(new WindowFunction<Tuple2<String, Long>, List, String, TimeWindow>() {
                    public void apply(String key, TimeWindow timeWindow, Iterable<Tuple2<String, Long>> iterable, Collector<List> collector) throws Exception {
                        List<Tuple2<String, Long>> list = new ArrayList<Tuple2<String, Long>>();
                        Iterator it = iterable.iterator();

                        while (it.hasNext()) {
                            list.add((Tuple2<String, Long>) it.next());
                        }
                        //按时间戳进行排序
                        list.sort(new Comparator<Tuple2<String, Long>>() {
                            @Override
                            public int compare(Tuple2<String, Long> o1, Tuple2<String, Long> o2) {
                                if (o1.f1 > o2.f1) {
                                    return 1;
                                } else {
                                    return -1;
                                }

                            }
                        });
                        long startTime = timeWindow.getStart();
                        long endTime = timeWindow.getEnd();
                        //System.out.println(format.format(startTime) + "  " + format.format(endTime));
                        /*for (Tuple2<String, Long> ss : list){
                            System.out.println(ss.f0+" "+ss.f1);
                        }*/
                        collector.collect(list);
                    }
                });
        //ds1.print();

        ds1.flatMap(new FlatMapFunction<List, Tuple2<String,Long>>() {
            @Override
            public void flatMap(List list, Collector<Tuple2<String,Long>> collector) throws Exception {
                for (Object t : list){
                    collector.collect((Tuple2<String,Long>)t);

                }
            }
        }).print();

        try {
            //System.out.println(env.getExecutionPlan());
            env.execute("sfda");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
