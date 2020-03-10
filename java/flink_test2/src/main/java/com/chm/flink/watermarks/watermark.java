package com.chm.flink.watermarks;


import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import javax.annotation.Nullable;


public class watermark {


    static AssignerWithPeriodicWatermarks assigner = new AssignerWithPeriodicWatermarks() {

        @Override
        public long extractTimestamp(Object logStr, long l) {
            JSONObject jsonObject = (JSONObject) JSONObject.parse((String) logStr);
            return Long.valueOf((String) jsonObject.get("timestamp"));
        }

        private final long maxTimeLag = 60000;//60 secs
        @Nullable
        @Override
        public Watermark getCurrentWatermark() { //设置允许延后60秒
            return new Watermark(System.currentTimeMillis() - maxTimeLag);
        }
    };

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //开启 EventTime 时间模型
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> ds = env.socketTextStream("47.52.108.102", 9000);
        //设置 window
        AllWindowedStream<String, TimeWindow> sw = ds.windowAll(SlidingEventTimeWindows.of(Time.minutes(2), Time.minutes(1)));

        SingleOutputStreamOperator<String> soo = sw.fold("a", new FoldFunction<String, String>() {
            @Override
            public String fold(String o, String o2) throws Exception {
                return o + o2;
            }
        });
        soo.print();
        try {
            env.execute("easfd");
        } catch (Exception e) {
            System.out.println("sf");
        }

    }
}
