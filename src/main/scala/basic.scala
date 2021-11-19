import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object basic {


  var user = ""
  var pass = ""
  var selection = 0
  def showMenu(username: String, password: String): Unit ={
  //basic user menu prints out information to enable the user to make a choice
    this.user = username
    this.pass = password
    //println(username)
    //println(password)
    //println(this.user+"b")
    //println(this.pass+"a")

    //admin user menu prints out information to enable the user to make a choice
    println(Console.BOLD + Console.GREEN + "Welcome basic user " + username + Console.RESET)
    println("")
    println(Console.BOLD + "1. Update user password")
    println(Console.BOLD + "2. Query the database")
    println(Console.BOLD + "0. Log out")
    selection = scala.io.StdIn.readInt()
    //println(selection)
    if (selection == 1) {
      connectionUtil.update_password(user, pass)
    } else if (selection == 2) {

    }else if(selection == 0){
      println(Console.GREEN+Console.BOLD+"LOGOUT SUCCESSFUL"+Console.RESET)
      login.login()


    }else{
      println(Console.RED+Console.BOLD+"THE ENTERED NUMBER IS NOT A VALID SELECTION PLEASE TRY AGAIN"+Console.RESET)
      showMenu(user, pass)
    }

  }

}
