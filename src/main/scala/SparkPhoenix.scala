import org.apache.spark.sql.SparkSession

/**
  *  spark与Phoenix整合.
  *  spark sql可以与hbase交互，比如说通过jdbc，但是实际使用时，一般是利用phoenix操作hbase。
  *  此时，需要在项目中引入phoenix-core-4.10.0-HBase-1.2.jar和phoenix-spark-4.10.0-HBase-1.2.jar(根据版本引入)。
  */
object SparkPhoenix {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
        .builder()
        .master("local[10]")
        .appName("a")
        .getOrCreate()

    var df = spark
        .read
        .format("org.apache.phoenix.spark")
        .option("table","tbl_order")
        .option("zkUrl","192.168.1.71:2181")
        .load()

    df.filter("USER_ID>1")
    df.show()


  }

}
