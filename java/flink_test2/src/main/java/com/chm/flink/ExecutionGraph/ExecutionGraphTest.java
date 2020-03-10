package com.chm.flink.ExecutionGraph;

import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ExecutionGraphTest {

    static ExecutionGraph executionGraph;

    public static void main(String[] args) {
        //executionGraph = ExecutionGraphBuilder.buildGraph();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    }

}
