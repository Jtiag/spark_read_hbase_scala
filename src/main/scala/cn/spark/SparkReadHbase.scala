package cn.spark

import java.sql.{Connection, DriverManager}

import com.mysql.jdbc.Driver
import org.apache.hadoop.hbase.client.{ConnectionFactory, Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.Base64
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jiangtao7 on 2017/12/5.
  */
object SparkReadHbase {
  private val hbase300Ip = "10.112.101.152:2181,10.112.101.151:2181,10.112.101.150:2181"
  private val znode300Parent = "/hbase-unsecure"
  private val hbase500Ip = "10.112.80.7"
  private val znode500Parent = "/hbase"
  private val hbase800Ip = "10.112.73.29:2181,10.112.73.30:2181,10.112.73.31:2181"
  private val znode800Parent = "/hbase"
  private val connJdbc = "jdbc:mysql://10.122.251.182:3306/test?user=jiangtao7&password=123"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkReadHbase").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val driver = classOf[Driver]
    val conn = DriverManager.getConnection(connJdbc)

    //    val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)
    // Execute Query
    //    val rs = statement.executeQuery("SELECT * FROM t_menu")
    //    // Iterate Over ResultSet
    //    while (rs.next()) {
    //      println(rs.getString("id")+" "+rs.getString("name")+" "+rs.getString("price"))
    //    }

    readHbase(sc, conn)
  }

  def readHbase(sc: SparkContext, connection: Connection): Unit = {

    val preparedStatement = connection.prepareStatement("INSERT INTO count_result(count)VALUES(?)")
    preparedStatement.setLong(1, 11111)
    preparedStatement.executeUpdate()
    val hconf = HBaseConfiguration.create()
    //    val tableName = "gome_sale"
//    val tableName = "VALUE_DATA_GOME"
    val tableName = "HAIER_DATA_GOME"
    hconf.set(TableInputFormat.INPUT_TABLE, tableName)
    hconf.set(HConstants.ZOOKEEPER_QUORUM, hbase800Ip)
    hconf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, znode800Parent)

    hconf.setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, 900000)
    hconf.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 9000000)
    hconf.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 9000000)

    val conn = ConnectionFactory.createConnection(hconf)

    val hAdmin = conn.getAdmin

    if (!hAdmin.tableExists(TableName.valueOf(tableName))) {
      println(tableName + " is not exists!!!")
      return
    }

    val scan = new Scan()
    /**
      * 获取列簇为values列名为gid的行
      */
//    scan.addColumn(Bytes.toBytes("values"), Bytes.toBytes("gid"))

    val proScan = ProtobufUtil.toScan(scan)
    val scanToString = Base64.encodeBytes(proScan.toByteArray)
    hconf.set(TableInputFormat.SCAN, scanToString)

    val immutableKeyPairRDD = sc.newAPIHadoopRDD(hconf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    val count = immutableKeyPairRDD.count()
    println("*********************************the result count is " + count + "  *************************************************")


    preparedStatement.setLong(1, count)
    preparedStatement.executeUpdate()
  }

  case class sqlTable(rowKey: String, col: String, value: String, timestamp: String)

}
