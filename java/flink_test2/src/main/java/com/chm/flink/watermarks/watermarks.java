package com.chm.flink.watermarks;


import com.alibaba.fastjson.JSONObject;
import com.chm.flink.Source_Sink.DataSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.Date;


public class watermarks {
    public static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    //固定watermark
    static AssignerWithPeriodicWatermarks assigner_1 = new AssignerWithPeriodicWatermarks() {


        public long extractTimestamp(Object logStr, long lon) {
            JSONObject json = (JSONObject) logStr;
            String wmtime= format.format(new Date(watermark.getTimestamp()));
            Long timestamp = (Long) json.get("timestamp");
            System.out.println(format.format(timestamp) + " | " + wmtime );
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

        private  long currentMaxTimestamp = 0L;

        public long extractTimestamp(Object logStr, long lon) {
            JSONObject json = (JSONObject) logStr;
            Long timestamp = (Long) json.get("timestamp");
            currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
            String wmtime= format.format(new Date(watermark.getTimestamp()));
            System.out.println(format.format(timestamp) + " | " + wmtime );
            json.put("watermark",wmtime);
            return Long.valueOf(timestamp);
        }
        //最大可延迟时间
        private final long maxTimeLag = 5000;
        public Watermark watermark;
        @Nullable
        public Watermark getCurrentWatermark() {
            watermark = new Watermark(currentMaxTimestamp - maxTimeLag);
            return watermark;
        }
    };


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        //env.enableCheckpointing(1000);
        //开启 EventTime 时间模型
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        SingleOutputStreamOperator<JSONObject> ds = env.addSource(new DataSource()).assignTimestampsAndWatermarks(assigner_2);
        ds.map(new MapFunction<JSONObject, Tuple2<JSONObject, Integer>>() {
            public Tuple2 map(JSONObject val) throws Exception {
                return Tuple2.of(val,1);
            }
        }).keyBy(new KeySelector<Tuple2<JSONObject, Integer>, String>() {
            public String getKey(Tuple2<JSONObject, Integer> val) throws Exception {
                return val.f0.getString("type");
            }
        }).timeWindow(Time.seconds(10), Time.seconds(5)).sum(1).print();
        env.execute("sdasff");


    }
}
