import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object admin {
  System.setProperty("hadoop.home.dir", "C:\\Program Files (x86)\\hadoop") //spark session for windows
  val spark = SparkSession
    .builder
    .appName("hello hive")
    .config("spark.master", "local")
    .enableHiveSupport()
    .getOrCreate()
  println("created spark session")
  spark.sparkContext.setLogLevel("ERROR")
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  Logger.getLogger("org.apache.spark").setLevel(Level.OFF)

  //set to admin option in menu
}
