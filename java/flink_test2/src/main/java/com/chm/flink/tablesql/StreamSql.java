package com.chm.flink.tablesql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;


import java.util.Random;

public class StreamSql {
    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStream<Order> orderA = env.addSource(new DataSource());
        DataStream<Order> orderB = env.addSource(new DataSource());

        // convert DataStream to Table
        Table tableA = tEnv.fromDataStream(orderA, "user, product, amount");


        // register DataStream as Table
        tEnv.registerDataStream("OrderB", orderB, "user, product, amount");

        // union the two tables
        Table result = tEnv.sqlQuery("SELECT * FROM " + tableA + " WHERE amount > 2 UNION ALL "
                + "SELECT * FROM OrderB WHERE amount < 2");

        //Table table = tEnv.sqlQuery("");

        tEnv.toAppendStream(result, Order.class).print();

        tEnv.execute("sf");
        //env.execute();
    }


    /**
     * Simple POJO.
     */
    public static class Order {
        public Long user;
        public String product;
        public int amount;

        public Order() {
        }

        public Order(Long user, String product, int amount) {
            this.user = user;
            this.product = product;
            this.amount = amount;
        }

        @Override
        public String toString() {
            return "Order{" +
                    "user=" + user +
                    ", product='" + product + '\'' +
                    ", amount=" + amount +
                    '}';
        }
    }

    public static class DataSource extends RichParallelSourceFunction<Order> {
        private volatile boolean isRunning = true;
        public void run(SourceContext<Order> ctx) throws Exception {
            Random random = new Random();
            while (isRunning) {
                Thread.sleep(2000);

                Order order = new Order();
                order.amount = random.nextInt(5);
                order.product = "" + (char)('A'+random.nextInt(5));
                order.user = random.nextLong();
                ctx.collect(order);
            }
        }
        public void cancel() {
            isRunning = false;
        }
    }
}
