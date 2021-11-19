object admin {

  var user = ""
  var pass = ""
  var selection = 0


  def showMenu(username: String, password: String): Unit = {
    this.user = username
    this.pass = password
    //println(username)
    //println(password)
    //println(this.user+"b")
    //println(this.pass+"a")

    //admin user menu prints out information to enable the user to make a choice
    println(Console.BOLD + Console.GREEN + "Welcome admin user " + username + Console.RESET)
    println("")
    println(Console.BOLD + "1. Update user password")
    println(Console.BOLD + "2. Make new new user")
    println(Console.BOLD + "3. Make current user an admin")
    println(Console.BOLD + "etc")
    selection = scala.io.StdIn.readInt()
    //println(selection)
    if (selection == 1) {
      connectionUtil.update_password(user, pass)
    } else if (selection == 2) {
      connectionUtil.make_new_user()
    } else if (selection == 3) {


    }else if (selection == 4){


    }else if(selection == 0){
      println(Console.GREEN+Console.BOLD+"LOGOUT SUCCESSFUL"+Console.RESET)
      login.login()


    }else{
      println(Console.RED+Console.BOLD+"THE ENTERED NUMBER IS NOT A VALID SELECTION PLEASE TRY AGAIN"+Console.RESET)

    }

  }

}
