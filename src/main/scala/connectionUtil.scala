import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, SQLIntegrityConstraintViolationException}
import jodd.util.BCrypt


object connectionUtil {
 //creates database connection using env variables

  val driver = "com.mysql.cj.jdbc.Driver"
  val url = sys.env("JDBC_DATABASE")
  val dbusername: String = sys.env("JDBC_USERNAME")
  val dbpassword: String = sys.env("JDBC_PASSWORD")
  //var connection: Connection = null
  var isAdmin = false
  var logged_in = false
  var update = false
  var success = false
  var made_admin = false
  Class.forName(driver)
  //connection = DriverManager.getConnection(url, username, password)

  /*function for logging in takes in username and password. It checks the database for the username and returns
  * encrypted password and removes the encryption before comparing it to the entered password */
  def userLogin(username: String, password: String): Boolean = {
    var connection: Connection = null
    try {
      connection = DriverManager.getConnection(url, dbusername, dbpassword)
      val prpStmt2: PreparedStatement = connection.prepareStatement("SELECT password, administrator from users WHERE username = ?")
      prpStmt2.setString(1, username)
      val savedSet = prpStmt2.executeQuery()
      while (savedSet.next()) {
        val passwordHash = savedSet.getString("password")
        val dbadmin = savedSet.getInt("administrator")
        if (dbadmin == 1) {
          isAdmin = true
        }else if(dbadmin == 0){
          isAdmin = false
        }
        if (BCrypt.checkpw(password, passwordHash)) {
          logged_in = true
          return logged_in
        }
        return logged_in
      }
      logged_in
    }
    catch {
      case e: Throwable => e.printStackTrace
        logged_in
    }
    finally {
      if(logged_in == false) {
        return logged_in}
      connection.close()

    }
  }

  //updates the users password, returns true if successful
  def update_password(username: String, password: String): Unit = {
    var connection: Connection = null
   // println("we got here")
    //println("username + password = "+username +""+password)
    try {
      connection = DriverManager.getConnection(url, dbusername, dbpassword)
      val prpStmt: PreparedStatement = connection.prepareStatement("SELECT password from users WHERE username = ?")
      prpStmt.setString(1, username)
      val savedSet = prpStmt.executeQuery()
       savedSet.next()
      val passwordHash = savedSet.getString("password")
      //println(password + " "+ currPassword)
      if (BCrypt.checkpw(password, passwordHash)) {
        println(Console.BOLD+Console.GREEN+"Information receive from the database"+Console.RESET)
        println("Enter new password:")
        val new_password = scala.io.StdIn.readLine
        val passwordHash = BCrypt.hashpw(new_password, BCrypt.gensalt)
        val prpStmt2: PreparedStatement = connection.prepareStatement("UPDATE users " + s"SET password = ? WHERE username = ?")
        prpStmt2.setString(1, passwordHash)
        prpStmt2.setString(2, username)
        prpStmt2.executeUpdate()
        prpStmt2.close
        update = true
        println(Console.BOLD+Console.GREEN+"UPDATE SUCCESSFUL! "+Console.RESET)
        println(Console.BOLD+Console.GREEN+"New login required!"+Console.RESET)
        login.login()
      }

    }
    catch {
      case e: Throwable => e.printStackTrace

    }
    finally {
    if(update == false && isAdmin == true) {
      println(Console.BOLD+Console.RED+"UPDATE FAILED!"+Console.RESET)
      }
      update = false
      admin.showMenu(username, password)
      connection.close()

    }
  }

  //creates new user and encrypts their password
  def make_new_user() = {
    var connection: Connection = null
    try {
      connection = DriverManager.getConnection(url, dbusername, dbpassword)
      //Make a new user based on user input
      println(Console.BOLD + "Enter in your username, it must be unique")
      val username = scala.io.StdIn.readLine()
      println(Console.BOLD + "Enter in your password, try to make it a good one")
      val password = scala.io.StdIn.readLine()
      val passwordHash = BCrypt.hashpw(password, BCrypt.gensalt)
      val insertsql = s"insert into users (username, password,administrator) values (?,?,0)"
      val preparedStmt: PreparedStatement = connection.prepareStatement(insertsql)
      preparedStmt.setString(1, username)
      preparedStmt.setString(2, passwordHash)
      preparedStmt.execute
      success = true
      preparedStmt.close
      println(Console.BOLD + Console.GREEN + "NEW USER SUCCESSFULLy ADDED!" + Console.RESET)
      admin.showMenu(admin.user, admin.pass)
    }
    catch {
      case e: Throwable => new SQLIntegrityConstraintViolationException
    }
    finally {
      if(success == false){
        println(Console.BOLD+Console.RED+"USERNAME ALREADY EXISTS IN THE DATABASE!"+Console.RESET)
        success = false
        admin.showMenu(admin.user, admin.pass)

      }
      connection.close()
    }
  }

  //allows the addmin to make another user and admin as long as they know that users name
  def make_admin(): Unit = {
    var connection: Connection = null
    try {
      connection = DriverManager.getConnection(url, dbusername, dbpassword)
      println("Please enter the username you want to make an admin")
      var updateUser = scala.io.StdIn.readLine()
      val prpStmt2: PreparedStatement = connection.prepareStatement("UPDATE users " + s"SET administrator = 1 WHERE username = ?")
      prpStmt2.setString(1, updateUser)
      prpStmt2.executeUpdate()
      prpStmt2.close
      made_admin = true
      println(Console.BOLD+Console.GREEN+"SUCCESSFULLY UPDATED!"+Console.RESET)
      admin.showMenu(admin.user, admin.pass)
    }
    catch {
      case e: Throwable => e.printStackTrace()
    }
    finally {
      if(made_admin == false){
        println(Console.BOLD+Console.RED+"UPDATE FAILED PLEASE TRY AGAIN!"+Console.RESET)
        success = false
        admin.showMenu(admin.user, admin.pass)

      }
      connection.close()
    }
  }

  }

