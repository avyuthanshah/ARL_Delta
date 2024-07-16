package example.Extra

import java.io.{File, PrintWriter}
import scala.io.Source

object status extends App{

  def writeFile(value:String):Unit={
    val filePath = "/home/avyuthan-shah/Desktop/F1Intern/DETasksSet#2/DeltaLake/delta/src/main/scala/example/Extra/status.txt"
    val writer = new PrintWriter(new File(filePath))
    writer.write(value)
    writer.close()
  }

  def readFile():Boolean={
    val filePath = "/home/avyuthan-shah/Desktop/F1Intern/DETasksSet#2/DeltaLake/delta/src/main/scala/example/Extra/status.txt"
    val source = Source.fromFile(filePath)
    val line = source.getLines().next()
    val readBooleanValue = line.toBoolean
    source.close()
    readBooleanValue
  }

}
