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

  //login.login()
   //connectionUtil.make_new_user()
  //println("Enter 2 to query county")
  //val user_int = readInt()

  login.login()
   //connectionUtil.make_new_user()


  spark.county_hesitancy(user_int)

  def make_stuff() {
    //make sure you have input folder and delete your metastore_db
    spark.county_pop()
    spark.vaccine_hesistancy()
    spark.state_death_cases()
    spark.county_pop2()
    spark.county_hesitancy(1)
    spark.state_hesitancy()
    spark.state_percent_hesitancy()
    spark.deaths_cases_since_june7()
    spark.state_pop()
    spark.deaths_cases_population()
    spark.deaths_cases_per100000()
    spark.deathspercap_vs_hesitancy()
  }
}
