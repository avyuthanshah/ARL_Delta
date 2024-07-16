package example.Extra

import java.time._
import java.time.format.DateTimeFormatter
import scala.util.Random
import java.sql.Timestamp

object time extends App{
  def getDate():String={
    val currentDate = LocalDate.now()
    // Format the date to yy-mm-dd format
    val formattedDate = currentDate.format(DateTimeFormatter.ofPattern("yy-MM-dd"))
    // Print the formatted date
    formattedDate
  }
  def getTime():String={
    val currentTime = LocalTime.now()
    // Format the date to yy-mm-dd format
    val formattedTime = currentTime.format(DateTimeFormatter.ofPattern("HH:mm:ss"))
    // Print the formatted date
    formattedTime
  }
  def getRandomTime(): String = {
    val random = new Random
    val hour = random.nextInt(24)     // Random hour between 0 and 23
    val minute = random.nextInt(60)   // Random minute between 0 and 59
    val second = random.nextInt(60)   // Random second between 0 and 59

    val formattedTime = LocalTime.of(hour, minute, second)
      .format(DateTimeFormatter.ofPattern("HH:mm:ss"))

    formattedTime
  }
  println(getTime())
}
