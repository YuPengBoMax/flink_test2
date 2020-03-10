package com.chm.flink.OperatorChain;

import com.chm.flink.Source_Sink.DataSource2;
import com.chm.flink.state.OperatorStateDemo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class OperatorChainTest {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env= StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //env.setParallelism(1);  //全局operator并行度
        //env.disableOperatorChaining();  //全局不运行进行chain操作
        //DataStream<Long> ds=env.fromElements(1L,2L,3L,4L,7L,5L,1L,5L,4L,6L,1L,7L,8L,9L,1L);
        DataStream<Long> ds = env.addSource(new DataSource2());
        ds.flatMap(new OperatorStateDemo.OperatorStateMap())
                .slotSharingGroup("aaa")  // 这里设置的是槽位组 在一个组的槽位可以进行共享槽位
                // 若不进行设置的,则是默认状态,若要进行槽位共享,则使用默认模式即可
                // 若设置了,则上游算子是其本身的模式,而下游算子则变为这个已经设置了的模式.
                .setParallelism(1).print();  //这里设置的并行度只是这个operator的并行度数量

        //System.out.println(env.getExecutionPlan());

        env.execute();

    }
}
