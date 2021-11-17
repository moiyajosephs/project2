
object login {

  def login() {

    /*communicates to the user what is needed to proceed and then takes in username and user password
    and then calls the login in function and passes it the values to check against the database
    to log in if the info provided is correct
    */

    //var admin = false
    //var logged_in = false

    println(Console.BOLD + "Please enter your username")

    var userName = scala.io.StdIn.readLine()
    var password = "password"

    if (userName != null) {

      println(Console.BOLD + "Please enter your password")
      password = scala.io.StdIn.readLine()

    }

    connectionUtil.login(userName, password)
  }
}





