import java.sql.DriverManager
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet

object connectionUtil {
  val driver = "com.mysql.cj.jdbc.Driver"
  val url = sys.env("JDBC_DATABASE")
  val username = sys.env("JDBC_USERNAME")
  val password = sys.env("JDBC_PASSWORD")
  var connection: Connection = null
  var admin = false
  var logged_in = false
  Class.forName(driver)
  connection = DriverManager.getConnection(url, username, password)

  def login(username: String, password: String): Boolean = {
    val prpStmt2: PreparedStatement = connection.prepareStatement("SELECT password, administrator from users WHERE username = ?")
    prpStmt2.setString(1, username)
    val savedSet = prpStmt2.executeQuery()
    while (savedSet.next()) {
      val dbpassword = savedSet.getString("password")
      val dbadmin = savedSet.getInt("administrator")
      if(dbadmin == 1) {admin = true}
      if (dbpassword == password) {
        logged_in = true
        return logged_in
      }
      else {
        logged_in = false
      }
    }
    return false
  }

  def update_password(username: String, password: String): Boolean = {
    //update password, return true if worked
    val prpStmt: PreparedStatement = connection.prepareStatement("SELECT password from users WHERE username = ?")
    prpStmt.setString(1, username)
    val savedSet = prpStmt.executeQuery()
    savedSet.next()
    val dbpassword = savedSet.getString("password")
    if (dbpassword == password) {
      println("Gotten from database" + dbpassword)
      println("Enter new password:")
      val new_password = readLine
      val prpStmt2: PreparedStatement = connection.prepareStatement("UPDATE users " + s"SET password = ? WHERE username = ?")
      prpStmt2.setString(1, new_password)
      prpStmt2.setString(2, username)
      prpStmt2.executeUpdate()
      prpStmt2.close
      return true
    }
    else {
      println("Failed to change password!")
      return false
    }
  }


  def make_new_user() = {
    //Make a new user based on user input
    println("Enter in your username, it must be unique")
    val username = scala.io.StdIn.readLine()
    println("Enter in your password, try to make it a good one")
    val password = scala.io.StdIn.readLine()

    val insertsql = s"insert into users (username, password,administrator) values (?,?,0)"
    val preparedStmt: PreparedStatement = connection.prepareStatement(insertsql)
    preparedStmt.setString(1, username)
    preparedStmt.setString(2, password)
    preparedStmt.execute
    preparedStmt.close

  }

  def make_admin(super_secret: String, session_user: String): Boolean = {
    //take in the super duper secret admin granting password
    var made_admin = false
    if (super_secret == "SUPER_SECRET") {
      try {
        val prpStmt2: PreparedStatement = connection.prepareStatement("UPDATE users " + s"SET administrator = 1 WHERE username = ?")
        prpStmt2.setString(1, session_user)
        prpStmt2.executeUpdate()
        prpStmt2.close
        made_admin = true
        println("Success new admin created!")
        return made_admin
      } catch {
        case e: Throwable => e.printStackTrace
          return false
      }
    } else {
      println("You entered the wrong super secret password, try again later")
      return made_admin
    }
  }


}
