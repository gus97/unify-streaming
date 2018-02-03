package cn.migu.scala.streaming

import java.util.Properties

import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.apache.spark.sql.types.StructType

import scala.collection.immutable.Map

object ScalaToJavaUtil {

  def getJDBCOptions(props: Properties,connectUrl:String,tableName:String): JDBCOptions = {

    val map = Map(
      "user" -> props.getProperty("user"),
      "password" -> props.getProperty("password"),
      "driver" -> props.getProperty("driver"),
      "batchsize" ->  props.getProperty("batchsize"))
    println("=========>>>>"+map)

    new JDBCOptions(connectUrl, tableName, map)
  }

  def saveTable(df: Dataset[Row],isCaseSensitive: Boolean, options: JDBCOptions)={
    JdbcUtils.saveTable(df,Some(df.schema),true,options)
  }
}
