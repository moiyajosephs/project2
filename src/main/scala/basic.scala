import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object basic {

  def showMenu(): Unit ={
  //basic user menu prints out information to enable the user to make a choice
    println("Please select an option from the list below")
    println("option 1")
    println("option 2")
    println("etc")

  }

  //creates spark session
  System.setProperty("hadoop.home.dir", "C:\\Program Files (x86)\\hadoop")
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
}
