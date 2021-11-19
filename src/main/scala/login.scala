

object login {


  def login() {

    /*communicates to the user what is needed to proceed and then takes in username and user password
    and then calls the login in function and passes it the values to check against the database
    to log in if the info provided is correct
    */

    //var isAdmin = false
    //var logged_in = false

    println(Console.BOLD + "Please enter your username")

    var username = scala.io.StdIn.readLine()
    var password = "password"

    if (username != null) {

      println(Console.BOLD + "Please enter your password")
      password = scala.io.StdIn.readLine()

    }

    connectionUtil.userLogin(username, password)


    //println("logged in status "+logged_in)
    //println("admin status "+isAdmin)
    if(connectionUtil.logged_in == true && connectionUtil.isAdmin == true){
      println(Console.BOLD+Console.GREEN+"LOGIN SUCCESSFUL!"+Console.RESET)
      //println("show menu"+username)
      //println("show menu"+password)
      connectionUtil.logged_in = false
      connectionUtil.isAdmin = false
      admin.showMenu(username, password)

    }
    else if(connectionUtil.logged_in == true && connectionUtil.isAdmin == false){
      println(Console.BOLD+Console.GREEN+"LOGIN SUCCESSFUL!"+Console.RESET)
      connectionUtil.logged_in = false
      basic.showMenu(username, password)

    }
    else if(connectionUtil.logged_in == false) {
        println(Console.BOLD+Console.RED+"INVALID USERNAME OR PASSWORD, TRY AGAIN"+Console.RESET)
        login()
      }

  }
}





