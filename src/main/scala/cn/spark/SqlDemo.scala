package cn.spark

import java.sql.DriverManager

import cn.spark.SparkReadHbase.connJdbc
import com.mysql.jdbc.Driver

/**
  * Created by jiangtao7 on 2017/12/5.
  */
object SqlDemo {
  val driver = classOf[Driver]
  private val connJdbc = "jdbc:mysql://10.122.251.151:3306/test?user=jiangtao7&password=123"

  def main(args: Array[String]): Unit = {
    val conn = DriverManager.getConnection(connJdbc)
    val statement = conn.createStatement()
    val sql = "CREATE TABLE IF NOT EXISTS count_result (count BIGINT)"
    statement.executeUpdate(sql)
    val preparedStatement = conn.prepareStatement("INSERT INTO count_result(count)VALUES(?)")

    preparedStatement.setLong(1,11111)
    preparedStatement.executeUpdate()

    preparedStatement.setLong(1, 100)
    preparedStatement.executeUpdate()
  }
}
