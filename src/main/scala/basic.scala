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

}
