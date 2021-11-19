import org.apache.log4j.{Level, Logger}

import scala.io.StdIn.readLine
import scala.io.StdIn.readInt
import java.sql.DriverManager
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import org.apache.spark.sql.SparkSession


object p2 extends App{

  //starts the application and prompts the user to login
  login.login()
   //connectionUtil.make_new_user()

}
