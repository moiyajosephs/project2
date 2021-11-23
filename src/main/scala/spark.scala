import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object spark {
  var user_input = ""
  var countyFIPS = ""
  var state = ""
  var hesitance = ""
  var order = ""
  var rank = ""
  var user = ""

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
  }

  def vaccine_hesistancy()= {
    //Choice 1: create table
    //Choice 2: search table for by FIPS
    spark.sql("DROP table if EXISTS vaccine_hesitancy")
    spark.sql("create table if not exists vaccine_hesitancy(FIPS varchar(5), county_name varchar(50), state_name varchar(50), percent_hesitant double, percent_strong_hesitant double, percent_vax double, percent_hispanic double, percent_asian double, percent_black double, percent_white double, state_code varchar(5)) row format delimited fields terminated by ',' ")
    spark.sql("LOAD DATA LOCAL INPATH 'input/vaccine_hesitancy.csv' INTO TABLE vaccine_hesitancy")
  }


  def state_death_cases() = {
    //don't need to show
    spark.sql("DROP table if EXISTS state_deaths_cases")
    spark.sql("create table if not exists state_deaths_cases(submission_date date, state_name varchar(20), tot_cases integer, conf_cases integer, tot_death integer, conf_death integer, created_at date) row format delimited fields terminated by ',' ")
    spark.sql("LOAD DATA LOCAL INPATH 'input/state_deaths_cases.csv' INTO TABLE state_deaths_cases")
  }

  def county_pop2() = {
    //don't need to show
    spark.sql("DROP VIEW IF EXISTS county_pop2")
    spark.sql("create view if not exists county_pop2 as (Select CONCAT(state, county) AS FIPS, stname, ctyname, popestimate2020 from county_pop)")
  }

  def county_hesitancy() = {
    //don't show
    spark.sql("DROP VIEW IF EXISTS county_hesitancy")
    spark.sql("create view if not exists county_hesitancy as (SELECT county_pop2.FIPS, county_pop2.stname AS STATE, county_pop2.ctyname AS COUNTY, county_pop2.popestimate2020 AS POPULATION_SIZE, vaccine_hesitancy.percent_hesitant AS PERCENT_HESITANT_UNSURE, vaccine_hesitancy.percent_strong_hesitant AS PERCENT_STRONG_HESITANT, vaccine_hesitancy.percent_hesitant * county_pop2.popestimate2020 AS HESITANT_UNSURE, vaccine_hesitancy.percent_strong_hesitant * county_pop2.popestimate2020 AS STRONG_HESITANT FROM county_pop2 INNER JOIN vaccine_hesitancy ON county_pop2.FIPS = vaccine_hesitancy.FIPS order by FIPS)")
  }

  def state_hesitancy() = {
    //don't show
    spark.sql("DROP VIEW IF EXISTS state_hesitancy")
    spark.sql("create view if not exists state_hesitancy as (SELECT STATE, sum(POPULATION_SIZE) as POPULATION_SIZE , sum(HESITANT_UNSURE) as HESITANT_UNSURE , sum(STRONG_HESITANT) as STRONG_HESITANT from county_hesitancy group by STATE)")
  }

  def state_percent_hesitancy() = {
    //don't show
    spark.sql("DROP VIEW IF EXISTS state_percent_hesitancy")
    spark.sql("create view if not exists state_percent_hesitancy as (SELECT STATE, HESITANT_UNSURE/POPULATION_SIZE AS PERCENT_HESITANT_UNSURE, STRONG_HESITANT/POPULATION_SIZE AS PERCENT_STRONG_HESITANT FROM state_hesitancy order by PERCENT_STRONG_HESITANT asc)")
  }

  def deaths_cases_since_june7() = {
    //don't show
    spark.sql("DROP VIEW IF EXISTS deaths_cases_since_june7")
    spark.sql("create view if not exists deaths_cases_since_june7 as (SELECT state_name AS STATE, min(tot_death) AS DEATHS_JUNE2021, max(tot_death) AS DEATHS_NOV2021, min(tot_CASES) AS CASES_JUNE2021, max(tot_cases) AS CASES_NOV2021 FROM state_deaths_cases WHERE submission_date > '2021-06-07' group by state_name)")
  }

  def state_pop() = {
    //dont' show
    spark.sql("DROP VIEW IF EXISTS state_pop")
    spark.sql("create view if not exists state_pop as (SELECT stname AS STATE, popestimate2020 AS POPULATION_ESTIMATE from county_pop where county = '000')")
  }

  def deaths_cases_population() {
    spark.sql("DROP VIEW IF EXISTS deaths_cases_population")
    spark.sql("create view if not exists deaths_cases_population as (SELECT state_pop.STATE, deaths_cases_since_june7.DEATHS_NOV2021-deaths_cases_since_june7.DEATHS_JUNE2021 as DEATHS_SINCE_JUNE7, deaths_cases_since_june7.CASES_NOV2021-deaths_cases_since_june7.CASES_JUNE2021 as CASES_SINCE_JUNE7, state_pop.POPULATION_ESTIMATE FROM deaths_cases_since_june7 JOIN state_pop ON deaths_cases_since_june7.STATE = state_pop.STATE)")
  }

  def deaths_cases_per100000() = {
    //show
    spark.sql("DROP VIEW IF EXISTS deaths_cases_per100000")
    spark.sql("create view if not exists deaths_cases_per100000 as (SELECT STATE, DEATHS_SINCE_JUNE7/POPULATION_ESTIMATE*100000 AS DEATHS_PER_100000, CASES_SINCE_JUNE7/POPULATION_ESTIMATE*100000 AS CASES_PER_100000 FROM deaths_cases_population)")
  }

  def deathspercap_vs_hesitancy() = {
    //show
    spark.sql("DROP VIEW IF EXISTS deathspercap_vs_hesitancy")
    spark.sql("create view if not exists deathspercap_vs_hesitancy as (SELECT state_percent_hesitancy.STATE AS STATE, round(state_percent_hesitancy.PERCENT_HESITANT_UNSURE*100,1) AS PERCENT_HESITANT_UNSURE, round(state_percent_hesitancy.PERCENT_STRONG_HESITANT*100,1) AS PERCENT_STRONG_HESITANT, round(deaths_cases_per100000.CASES_PER_100000, 0) AS CASES_PER_100000, round(deaths_cases_per100000.DEATHS_PER_100000, 0) AS DEATHS_PER_100000 FROM deaths_cases_per100000 JOIN state_percent_hesitancy ON deaths_cases_per100000.STATE = state_percent_hesitancy.STATE)")
  }

  def showQueryMenu(userType: String): Unit ={
    user = userType

    println("COVID-19 VACCINE HESITANCY, CASES, DEATHS ANALYSIS TOOL")
    println("----------------------------------")
    println(s"1) Show estimated vaccine hesitancy data for your county")
    println(s"2) Show estimated vaccine hesitancy data for your state")
    println(s"3) Show total covid cases/deaths in your state")
    println(s"4) Show covid cases/deaths per capita for states with the greatest/least amount of strong vaccine hesitancy")
    println(s"5) Show covid cases/deaths per capita for states with the greatest/least amount of vaccine uncertainty")
    println(s"0) Logout")
    println("----------------------------------")
    println("Enter your option:")
    user_input = scala.io.StdIn.readLine()
    if(user_input == "1") {
      query1()
    } else if(user_input == "2") {
      query2()
    } else if(user_input == "3") {
      query3()
    } else if(user_input == "4") {
      query4()
    } else if(user_input == "5") {
      query5()
    }else if(user_input == "0"){
      println(Console.GREEN+Console.BOLD+"LOGOUT SUCCESSFUL"+Console.RESET)
      login.login()
    } else {
      println("Invalid entry: Please enter a number 1-5")
      showQueryMenu(user)
    }
  }

  def query1() = {
    println("Please enter 5-digit county FIPS code:")
    countyFIPS = scala.io.StdIn.readLine()
    println("COUNTY VACCINE HESITANCY")
    spark.sql(s"Select STATE, COUNTY, POPULATION_SIZE, round(PERCENT_HESITANT_UNSURE * 100, 1) AS PERCENT_HESITANT_UNSURE, round(PERCENT_STRONG_HESITANT * 100) AS PERCENT_STRONG_HESITANT from county_hesitancy where FIPS = '$countyFIPS'").show
    if(user == "basic")
    {
      toMenu()
    }

    println("Would you like to save the data as a CSV file? Yes/No")
    var saveFile = scala.io.StdIn.readLine()

    if(saveFile.toUpperCase() == "YES") {
      spark.sql(s"Select STATE, COUNTY, POPULATION_SIZE, round(PERCENT_HESITANT_UNSURE * 100, 1) AS PERCENT_HESITANT_UNSURE, round(PERCENT_STRONG_HESITANT * 100) AS PERCENT_STRONG_HESITANT from county_hesitancy where FIPS = '$countyFIPS'").write.format("csv").mode("overwrite").save(s"plotly/unsurebycounty")
      println("Data has been successfully saved!")
    }
    else{
      toMenu()
    }
    //showQueryMenu()
    toMenu()
  }


  def query2() = {
    println("Please enter state:")
    state = scala.io.StdIn.readLine()
    println("STATE VACCINE HESITANCY")
    spark.sql(s"Create view if not exists state_data as (Select state_pop.STATE, state_pop.POPULATION_ESTIMATE AS STATE_POPPULATION_ESTIMATE, round(PERCENT_HESITANT_UNSURE * 100, 1) AS PERCENT_HESITANT_UNSURE, round(PERCENT_STRONG_HESITANT * 100) AS PERCENT_STRONG_HESITANT from state_percent_hesitancy JOIN state_pop on state_percent_hesitancy.STATE = state_pop.STATE)")
    spark.sql(s"Select * from state_data where state = '$state'").show
    if(user == "basic")
    {
      toMenu()
    }

    println("Would you like to save the data as a CSV file? Yes/No")
    var saveFile = scala.io.StdIn.readLine()

    if(saveFile.toUpperCase() == "YES") {
      spark.sql(s"Select * from state_data where state = '$state'").write.format("csv").mode("overwrite").save(s"plotly/unsurebystate")
      println("Data has been successfully saved!")
    }
    else{
      toMenu()
    }
    //showQueryMenu()
    toMenu()
  }



  def query3() = {
    println("Please enter state:")
    state = scala.io.StdIn.readLine()
    println("STATE CASE AND DEATH TOTALS")
    spark.sql(s"Select STATE, CASES_NOV2021 AS TOTAL_CASES, DEATHS_NOV2021 AS TOTAL_DEATHS from deaths_cases_since_june7 where STATE = '$state'").show
    if(user == "basic")
    {
      toMenu()
    }

    println("Would you like to save the data as a CSV file? Yes/No")
    var saveFile = scala.io.StdIn.readLine()

    if(saveFile.toUpperCase() == "YES") {
      spark.sql(s"Select STATE, CASES_NOV2021 AS TOTAL_CASES, DEATHS_NOV2021 AS TOTAL_DEATHS from deaths_cases_since_june7 where STATE = '$state'").write.format("csv").mode("overwrite").save(s"plotly/casebystate")
      println("Data has been successfully saved!")
    }
    else{
      toMenu()
    }
    //showQueryMenu()
    toMenu()
  }

  def query4() = {
    greatestOrLeast()
    println(s"COVID CASES/DEATHS PER CAPITA FOR STATES WITH THE $rank PERCENTAGE OF $hesitance INDIVIDUALS")
    spark.sql(s"select STATE, PERCENT_STRONG_HESITANT, CASES_PER_100000, DEATHS_PER_100000 from deathspercap_vs_hesitancy order by PERCENT_STRONG_HESITANT $order limit 10 ").show

    if(user == "basic")
      {
        toMenu()
      }

    println("Would you like to save the data as a CSV file? Yes/No")
    var saveFile = scala.io.StdIn.readLine()

    if(saveFile.toUpperCase() == "YES") {
      spark.sql(s"select STATE, PERCENT_STRONG_HESITANT, CASES_PER_100000, DEATHS_PER_100000 from deathspercap_vs_hesitancy order by PERCENT_STRONG_HESITANT $order limit 10 ").write.format("csv").mode("overwrite").save(s"plotly/greatestorleast")
      println("Data has been successfully saved!")
      }
      else{
      toMenu()
    }
    //showQueryMenu()
    toMenu()

  }
  def query5() = {
    greatestOrLeast()
    println(s"COVID CASES/DEATHS PER CAPITA FOR STATES WITH THE $rank PERCENTAGE OF $hesitance INDIVIDUALS")
    spark.sql(s"select STATE, PERCENT_HESITANT_UNSURE, CASES_PER_100000, DEATHS_PER_100000 from deathspercap_vs_hesitancy order by PERCENT_HESITANT_UNSURE $order limit 10 ").show

    if (user == "basic") {
      toMenu()
    }

    println("Would you like to save the data as a CSV file? Yes/No")
    var saveFile = scala.io.StdIn.readLine()

    if (saveFile.toUpperCase() == "YES") {
      spark.sql(s"select STATE, PERCENT_HESITANT_UNSURE, CASES_PER_100000, DEATHS_PER_100000 from deathspercap_vs_hesitancy order by PERCENT_HESITANT_UNSURE $order limit 50").write.format("csv").mode("overwrite").save(s"plotly/unsure")

      println("Data has been successfully saved!")
    }
    else {
      toMenu()
    }

    toMenu()
  }

  def greatestOrLeast(): Unit = {
    println("Would you like to see covid cases/deaths per capita for states with the greatest or least amount of strong vaccine hesitancy")
    println(s"1) Greatest")
    println(s"2) Least")
    user_input = scala.io.StdIn.readLine()
    //println(user_input)
    if(user_input == "1") {
      order = "desc"
      rank = "GREATEST"
      hesitance = "STRONGLY HESITANT"
    } else if (user_input == "2") {
      order = "asc"
      rank = "LOWEST"
      hesitance = "HESITANT OR UNSURE"
    } else {
      println("Wrong input. Please enter 1 or 2")
    }


  }
  def toMenu(): Unit = {
    println(Console.BOLD+"Press enter key to return to the query menu")
    var buttonPress = scala.io.StdIn.readLine()
    if(buttonPress == "") {
      showQueryMenu(user)
    }else if(buttonPress != ""){
      println(Console.BOLD+Console.RED+"ANY INPUT WORKS FOR PEOPLE WHO WANT TO IGNORE INSTRUCTIONS"+Console.RESET)


    }

  }
}
