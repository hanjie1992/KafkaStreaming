import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * spark 通过 hbase API 写入hbase
  */
object SparkHbase {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
        .builder()
        .appName("SparkHbase")
        .master("local[10]")
        .enableHiveSupport()
        .getOrCreate()

    sparkSession.sql("use " + "guiyang")//ipva
    val sqlD: DataFrame = getlineScheduleAlgorithm(sparkSession)
    val scheduleAlgorithmRDD= sqlD.rdd
    scheduleAlgorithmRDD.foreachPartition{ records =>
      val config = HBaseConfiguration.create
      config.set("hbase.zookeeper.property.clientPort", "2181")
      config.set("hbase.zookeeper.quorum", "192.168.1.81,192.168.1.82,192.168.1.83")
      val connection = ConnectionFactory.createConnection(config)
      val table = connection.getTable(TableName.valueOf("line_schedule_algorithm_test"))

      val list = new java.util.ArrayList[Put]
      records.foreach(t =>{
        val put = new Put(Bytes.toBytes(t.toString))
//        put.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("lineId"),Bytes.toBytes(t(0).toString))
//        put.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("lineName"),Bytes.toBytes(t(1).toString))
        put.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("line_year"),Bytes.toBytes(t(0).toString))
        put.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("line_month"),Bytes.toBytes(t(1).toString))
        put.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("line_day"),Bytes.toBytes(t(2).toString))
        put.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("line_date_type"),Bytes.toBytes(t(3).toString))
        put.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("line_id"),Bytes.toBytes(t(4).toString))
        put.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("line_name"),Bytes.toBytes(t(5).toString))
        put.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("line_direction"),Bytes.toBytes(t(6).toString))
        put.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("line_type"),Bytes.toBytes(t(7).toString))
        put.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("mileage"),Bytes.toBytes(t(8).toString))
        put.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("line_time"),Bytes.toBytes(t(9).toString))
        put.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("line_on_count"),Bytes.toBytes(t(10).toString))
        put.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("line_sec_time"),Bytes.toBytes(t(11).toString))
        list.add(put)
      })
      table.put(list)
      table.close()
    }
  }

  def getHanJieTest(sparkSession: SparkSession):DataFrame = {
    val sqlStr =
      """
        |select line_id lineId,line_name lineName from app_line_schedule_algorithm
        |where line_year=2019 and line_month=9  group by line_id,line_name limit 20
      """.stripMargin
    sparkSession.sql(sqlStr)
  }

  def getlineScheduleAlgorithm(sparkSession: SparkSession): DataFrame = {
    val sqlStr =
      s"""
         |select
         |  cast(line_year as string),
         |  cast(line_month as string),
         |  cast(line_day as string),
         |  cast(line_date_type as string),
         |  cast(line_id as string),
         |  cast(line_name as string),
         |  cast(line_direction as string),
         |  cast(line_type as string),
         |  cast(mileage as string),
         |  cast(line_time as string),
         |  cast(line_on_count as string),
         |  cast(line_sec_time as string)
         |from app_line_schedule_algorithm
         |where line_year=2019 and line_month=9
      """.stripMargin
    sparkSession.sql(sqlStr)
  }

}
