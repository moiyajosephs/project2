import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object admin {

  var user = ""
  var pass = ""
  var selection = 0

  def showMenu(username: String, password: String): Unit ={
    this.user = username
    this.pass = password
    //println(username)
    //println(password)
    //println(this.user+"b")
    //println(this.pass+"a")

    //admin user menu prints out information to enable the user to make a choice
    println(Console.BOLD+Console.GREEN+"Welcome admin user "+username+Console.RESET)
    println("")
    println(Console.BOLD+"1. Update user password")
    println(Console.BOLD+"2. Make new new user")
    println(Console.BOLD+"3. Make current user an admin")
    println(Console.BOLD+"etc")
    selection = scala.io.StdIn.readInt()
    //println(selection)
  if(selection == 1){
    connectionUtil.update_password(user, pass)
  }else if(selection == 2){
    connectionUtil.make_new_user()
  }else if(selection == 3){


  }

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
