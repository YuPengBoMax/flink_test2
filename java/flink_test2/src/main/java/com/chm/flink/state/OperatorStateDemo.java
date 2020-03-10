package com.chm.flink.state;

import com.chm.flink.Source_Sink.DataSource2;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Operator State Demo
 *
 * 1 2 3 4 7 5 1 5 4 6 1 7 8 9 1
 *
 * 输出如下：
 * (5,2 3 4 7 5)
 * (3,5 4 6)
 * (3,7 8 9)
 */
public class OperatorStateDemo {


    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env= StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        DataStream<Long> ds = env.addSource(new DataSource2());
        ds.flatMap(new OperatorStateMap()).print();  //这里设置的并行度只是这个operator的并行度数量

        //System.out.println(env.getExecutionPlan());
        env.execute();
    }


    public static class OperatorStateMap extends RichFlatMapFunction<Long, Tuple2<Integer,String>> implements CheckpointedFunction {

        //托管状态
        private ListState<Long> listState;
        //原始状态
        private List<Long> listElements;


        @Override
        public void flatMap(Long value, Collector collector) throws Exception {
            if(value==1){
                if(listElements.size()>0){
                    StringBuffer buffer=new StringBuffer();
                    for(Long ele:listElements){
                        buffer.append(ele+" ");
                    }
                    int sum=listElements.size();
                    collector.collect(new Tuple2<Integer,String>(sum,buffer.toString()));
                    listElements.clear();
                }
            }else{
                listElements.add(value);
            }
        }

        /**
         * 进行checkpoint进行快照
         * @param context
         * @throws Exception
         */
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            listState.clear();
            for(Long ele:listElements){
                listState.add(ele);
            }
        }

        /**
         * state的初始状态，包括从故障恢复过来
         * @param context
         * @throws Exception
         */
        public void initializeState(FunctionInitializationContext context) throws Exception {
            ListStateDescriptor listStateDescriptor=new ListStateDescriptor("checkPointedList",
                    TypeInformation.of(new TypeHint<Long>() {}));
            listState=context.getOperatorStateStore().getListState(listStateDescriptor);
            //如果是故障恢复
            if(context.isRestored()){
                //从托管状态将数据到移动到原始状态
                for(Long ele:listState.get()){
                    listElements.add(ele);
                }
                listState.clear();
            }
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            listElements=new ArrayList<Long>();
        }
    }

}
