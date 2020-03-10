package com.chm.flink.mysql;


import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class JdbcReader extends RichSourceFunction<Tuple3<Integer,String,Long>> {
    private static final Logger logger = LoggerFactory.getLogger(JdbcReader.class);

    private Connection connection = null;
    private PreparedStatement ps = null;



    String username = "root";
    String password = "123456";
    String url = "jdbc:mysql://localhost:3306/flink_db?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC&autoReconnect=true&useSSL=false&user="+username+"&password="+password;
    String driver = "com.mysql.cj.jdbc.Driver";
    String sql = "select *  from wc";


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName(driver);
        connection = DriverManager.getConnection(url);
        ps = connection.prepareStatement(sql);
    }

    //执行查询并获取结果
    @Override
    public void run(SourceContext<Tuple3<Integer,String,Long>> ctx) throws Exception {
        try {
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                Integer id = resultSet.getInt("id");
                String word = resultSet.getString("word");
                Long frequency = resultSet.getLong("frequency");
                logger.error("readJDBC name:{}", word);
                Tuple3<Integer,String,Long> tuple3 = new Tuple3<>();
                tuple3.setFields(id,word, frequency);
                ctx.collect(tuple3);
            }
        } catch (Exception e) {
            logger.error("runException:{}", e);
        }

    }

    //关闭数据库连接
    @Override
    public void cancel() {
        try {
            super.close();
            if (connection != null) {
                connection.close();
            }
            if (ps != null) {
                ps.close();
            }
        } catch (Exception e) {
            logger.error("runException:{}", e);
        }
    }
}
