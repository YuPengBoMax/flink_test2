package com.chm.flink.mysql;


import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;


public class JdbcWriter extends RichSinkFunction<Tuple3<Integer,String,Long>> {
    private Connection connection;
    private PreparedStatement preparedStatement;

    String username = "root";
    String password = "123456";
    String url = "jdbc:mysql://localhost:3306/flink_db?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC&autoReconnect=true&useSSL=false&user="+username+"&password="+password;
    String driver = "com.mysql.cj.jdbc.Driver";
    String sql = "insert into wc(id,word,frequency) VALUES(?,?,?)";

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 加载JDBC驱动
        Class.forName(driver);
        // 获取数据库连接
        connection = DriverManager.getConnection(url);//写入mysql数据库
        preparedStatement = connection.prepareStatement(sql);//insert sql在配置文件中
        super.open(parameters);
    }


    public void invoke(Tuple3<Integer,String,Long> value, Context context) throws Exception {
        try {
            Integer id = value.getField(0);
            String word = value.getField(1);
            Long frequency = value.getField(2);
            preparedStatement.setInt(1,id);
            preparedStatement.setString(2,word);
            preparedStatement.setLong(3,frequency);
            preparedStatement.executeUpdate();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if(preparedStatement != null){
            preparedStatement.close();
        }
        if(connection != null){
            connection.close();
        }
        super.close();
    }
}
