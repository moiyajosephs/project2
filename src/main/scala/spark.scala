import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object spark {

  def showMenu(): Unit ={
    // do something




  }

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

  def county_pop() = {
    //Dont need to show
    spark.sql("DROP table if EXISTS county_pop")
    spark.sql("create table if not exists county_pop(state char(2), county char(3), stname varchar(20), ctyname varchar(50), popestimate2020 integer) row format delimited fields terminated by ',' ")
    spark.sql("LOAD DATA LOCAL INPATH 'input/co_est2020.csv' INTO TABLE county_pop")
    spark.sql("SELECT * FROM county_pop").show()
  }

  def vaccine_hesistancy()= {
    //Choice 1: create table
    //Choice 2: search table for by FIPS
    spark.sql("DROP table if EXISTS vaccine_hesitancy")
    spark.sql("create table if not exists vaccine_hesitancy(FIPS varchar(5), county_name varchar(50), state_name varchar(50), percent_hesitant double, percent_strong_hesitant double, percent_vax double, percent_hispanic double, percent_asian double, percent_black double, percent_white double, state_code varchar(5)) row format delimited fields terminated by ',' ")
    spark.sql("LOAD DATA LOCAL INPATH 'input/vaccine_hesitancy.csv' INTO TABLE vaccine_hesitancy")
    spark.sql("SELECT * FROM vaccine_hesitancy").show()
  }



  def state_death_cases() = {
    //don't need to show
    spark.sql("DROP table if EXISTS state_deaths_cases")
    spark.sql("create table if not exists state_deaths_cases(submission_date date, state_name varchar(20), tot_cases integer, conf_cases integer, tot_death integer, conf_death integer, created_at date) row format delimited fields terminated by ',' ")
    spark.sql("LOAD DATA LOCAL INPATH 'input/state_deaths_cases.csv' INTO TABLE state_deaths_cases")
    spark.sql("SELECT * FROM state_deaths_cases").show()
  }

  def county_pop2() = {
    //don't need to show
    spark.sql("DROP VIEW IF EXISTS county_pop2")
    spark.sql("create view if not exists county_pop2 as (Select CONCAT(state, county) AS FIPS, stname, ctyname, popestimate2020 from county_pop)")
    spark.sql("SELECT * FROM county_pop2").show()
  }

  def county_hesitancy(a:Int) = {
    //don't show
      if(a==1) {
     //   spark.sql("DROP VIEW IF EXISTS county_hesitancy")
       // spark.sql("create view if not exists county_hesitancy as (SELECT county_pop2.FIPS, county_pop2.stname AS STATE, county_pop2.ctyname AS COUNTY, county_pop2.popestimate2020 AS POPULATION_SIZE, vaccine_hesitancy.percent_hesitant AS PERCENT_HESITANT_UNSURE, vaccine_hesitancy.percent_strong_hesitant AS PERCENT_STRONG_HESITANT, vaccine_hesitancy.percent_hesitant * county_pop2.popestimate2020 AS HESITANT_UNSURE, vaccine_hesitancy.percent_strong_hesitant * county_pop2.popestimate2020 AS STRONG_HESITANT FROM county_pop2 INNER JOIN vaccine_hesitancy ON county_pop2.FIPS = vaccine_hesitancy.FIPS order by FIPS)")
        spark.sql("SELECT * FROM county_hesitancy").show()
      }
      else if(a==2){
        println("Enter the county")
        var user_input = scala.io.StdIn.readLine()
        spark.sql(s"Select * from county_hesitancy where FIPS = $user_input").show()
      }
      else if(a==3){
        println("Enter the state")
        var user_input = scala.io.StdIn.readLine()
        spark.sql(s"Select * from county_hesitancy where STATE = '$user_input'").show()
      }
    else{
        println("Not a valid query")
      }
  }

  def state_hesitancy() = {
    //don't show
    spark.sql("DROP VIEW IF EXISTS state_hesitancy")
    spark.sql("create view if not exists state_hesitancy as (SELECT STATE, sum(POPULATION_SIZE) as POPULATION_SIZE , sum(HESITANT_UNSURE) as HESITANT_UNSURE , sum(STRONG_HESITANT) as STRONG_HESITANT from county_hesitancy group by STATE)")
    spark.sql("SELECT * FROM state_hesitancy").show()
  }

  def state_percent_hesitancy() = {
    //don't show
    spark.sql("DROP VIEW IF EXISTS state_percent_hesitancy")
    spark.sql("create view if not exists state_percent_hesitancy as (SELECT STATE, HESITANT_UNSURE/POPULATION_SIZE AS PERCENT_HESITANT_UNSURE, STRONG_HESITANT/POPULATION_SIZE AS PERCENT_STRONG_HESITANT FROM state_hesitancy order by PERCENT_STRONG_HESITANT asc)")
    spark.sql("SELECT * FROM state_percent_hesitancy").show()
  }

  def deaths_cases_since_june7() = {
    //don't show
    spark.sql("DROP VIEW IF EXISTS deaths_cases_since_june7")
    spark.sql("create view if not exists deaths_cases_since_june7 as (SELECT state_name AS STATE, min(tot_death) AS DEATHS_JUNE2021, max(tot_death) AS DEATHS_NOV2021, min(tot_CASES) AS CASES_JUNE2021, max(tot_cases) AS CASES_NOV2021 FROM state_deaths_cases WHERE submission_date > '2021-06-07' group by state_name)")
    spark.sql("SELECT * FROM deaths_cases_since_june7").show()
  }

  def state_pop() = {
    //dont' show
    spark.sql("DROP VIEW IF EXISTS state_pop")
    spark.sql("create view if not exists state_pop as (SELECT stname AS STATE, popestimate2020 AS POPULATION_ESTIMATE from county_pop where county = '000')")
    spark.sql("SELECT * FROM state_pop").show()
  }


  def deaths_cases_population() {
    spark.sql("DROP VIEW IF EXISTS deaths_cases_population")
    spark.sql("create view if not exists deaths_cases_population as (SELECT state_pop.STATE, deaths_cases_since_june7.DEATHS_NOV2021-deaths_cases_since_june7.DEATHS_JUNE2021 as DEATHS_SINCE_JUNE7, deaths_cases_since_june7.CASES_NOV2021-deaths_cases_since_june7.CASES_JUNE2021 as CASES_SINCE_JUNE7, state_pop.POPULATION_ESTIMATE FROM deaths_cases_since_june7 JOIN state_pop ON deaths_cases_since_june7.STATE = state_pop.STATE)")
    spark.sql("SELECT * FROM deaths_cases_population").show()
  }

  def deaths_cases_per100000() = {
    //show
    spark.sql("DROP VIEW IF EXISTS deaths_cases_per100000")
    spark.sql("create view if not exists deaths_cases_per100000 as (SELECT STATE, DEATHS_SINCE_JUNE7/POPULATION_ESTIMATE*100000 AS DEATHS_PER_100000, CASES_SINCE_JUNE7/POPULATION_ESTIMATE*100000 AS CASES_PER_100000 FROM deaths_cases_population)")
    spark.sql("SELECT * FROM deaths_cases_per100000").show()
  }

  def deathspercap_vs_hesitancy() = {
    //show
    spark.sql("DROP VIEW IF EXISTS deathspercap_vs_hesitancy")
    spark.sql("create view if not exists deathspercap_vs_hesitancy as (SELECT state_percent_hesitancy.STATE AS STATE, round(state_percent_hesitancy.PERCENT_HESITANT_UNSURE*100,1) AS PERCENT_HESITANT_UNSURE, round(state_percent_hesitancy.PERCENT_STRONG_HESITANT*100,1) AS PERCENT_STRONG_HESITANT, round(deaths_cases_per100000.CASES_PER_100000, 0) AS CASES_PER_100000, round(deaths_cases_per100000.DEATHS_PER_100000, 0) AS DEATHS_PER_100000 FROM deaths_cases_per100000 JOIN state_percent_hesitancy ON deaths_cases_per100000.STATE = state_percent_hesitancy.STATE)")
    spark.sql("SELECT * FROM deathspercap_vs_hesitancy").show()
  }


}
