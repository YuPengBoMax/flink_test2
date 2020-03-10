package com.chm.flink.test

import java.sql
import java.sql.Connection
import java.util.Properties

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.collector.selector.OutputSelector
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.api.scala._

/**
  * Flink消费多个topic的数据,sink到不同的表
  */
object MoreTopic {
  private val broker = "***"
  private val group_id = "***"
  private val topic = "***"

  def main(args: Array[String]): Unit = {
    //获取流的环境;
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(8)
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", broker)
    properties.setProperty("group.id", group_id)
    // 设置动态监测topic和paritition的变化
    properties.setProperty("flink.partition-discovery.interval-millis","1000")
    // 用正则匹配符合条件的多个topic
    val consumer = new FlinkKafkaConsumer010[String](java.util.regex.Pattern.compile("jason-test-[0-9]"), new SimpleStringSchema, properties)
    // 设置从最新的处开始消费
    consumer.setStartFromLatest()
    val datastream = env.addSource(consumer)
      .filter(_.nonEmpty)
      .flatMap(_.split(","))
      .map(_.trim.toString)
    datastream.map((_,1)).disableChaining()
      .addSink(new MySQLSink_Topic("a"))
      .name("mysql_sink_before_a")
    datastream.map((_,1)).disableChaining()
      .addSink(new MySQLSink_Topic("b"))
      .name("mysql_sink_before_b")
    // 拆分流,把流拆分成一个包含hello,一个包含jason的流
    val split_ds = datastream
      .rebalance
      .split(new OutputSelector[String]{
        override def select(value: String) = {
          val list = new java.util.ArrayList[String]()
          if(value.contains("hello")){
            list.add("h")
          }else{
            list.add("j")
          }
          list
        }
      })
    // 如果想要后面的4个sink在DAG图显示并行,需要在最后一个算子上禁用operator chain,或者设置一个和上面不一样的并行度
    val h_ds = split_ds.select("h").map((_,1)).setParallelism(1)
    val j_ds = split_ds.select("j").map((_,1)).setParallelism(1)

    h_ds.addSink(new MySQLSink_Topic("a")).name("h_ds_mysql_sink")
    j_ds.addSink(new MySQLSink_Topic("b")).name("j_ds_mysql_sink")

    val httpHosts  = new java.util.ArrayList[HttpHost]()
    // http:port 9200 tcp:port 9300
    httpHosts.add(new HttpHost("***", 0, "http"))
    httpHosts.add(new HttpHost("***", 0, "http"))
    httpHosts.add(new HttpHost("***", 0, "http"))
    val elasticsearchSink = new ElasticsearchSinkFunction[(String,Int)] {
      override def process(element: (String,Int), ctx: RuntimeContext, indexer: RequestIndexer) {
        val json = new java.util.HashMap[String, String]()
        json.put("name", element._1)
        json.put("count", element._2.toString)
        println("要写入es的内容: " + element)
        val r = Requests.indexRequest.index("**").`type`("**").source(json,XContentType.JSON)
        indexer.add(r)
      }
    }

    val esSinkBuilder = new ElasticsearchSink.Builder[(String,Int)](httpHosts, elasticsearchSink)
    // 设置刷新前缓冲的最大算子操作量
    esSinkBuilder.setBulkFlushMaxActions(1)

    h_ds.addSink(esSinkBuilder.build()).name("h_ds_es-sink")
    j_ds.addSink(esSinkBuilder.build()).name("j_ds_es-sink")

    env.execute("Multiple Topic Multiple Sink")
  }
}

/**
  * 把结果保存到mysql里面
  */
class MySQLSink_Topic(table: String) extends RichSinkFunction[(String,Int)] with Serializable {
  var connection: sql.Connection = _
  var ps: sql.PreparedStatement = _
  var statement: java.sql.Statement = _
  val username = "***"
  val password = "***"
  val drivername = "***"
  val url = "***"
  private var conn: Connection  = _
  /**
    * 打开mysql的连接
    * @param parameters
    */
  override def open(parameters: Configuration): Unit = {
    conn = getConn(new BasicDataSource)
    val sql = "insert into "+ table +"(name,count) values(?,?)"
    println(sql)
    ps = conn.prepareStatement(sql)
  }

  /**
    * 处理数据后写入mysql
    * @param value
    */
  override def invoke(row: (String,Int)): Unit = {
    ps.setString(1,row._1)
    ps.setLong(2,row._2)
    ps.execute()
  }

  /**
    * 关闭mysql的连接
    */
  override def close(): Unit = {
    if (ps != null) {
      ps.close()
    }
    if (connection != null) {
      connection.close()
    }
  }

  /**
    * 创建mysql的连接池
    * @param ds
    * @return
    */
  def getConn(ds: BasicDataSource): Connection = {
    ds.setDriverClassName("com.mysql.jdbc.Driver");
    ds.setUrl("jdbc:mysql://master:3306/test");
    ds.setUsername("mysql");
    ds.setPassword("12345678");
    ds.setInitialSize(10);
    ds.setMaxTotal(15);
    ds.setMinIdle(10);
    var con: Connection = null
    con = ds.getConnection
    con
  }
}