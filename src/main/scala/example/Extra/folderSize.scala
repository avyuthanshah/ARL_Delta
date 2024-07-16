package example.Extra
import java.nio.file.{Files, Paths}

object folderSize extends App{
  def getCurrentFolderSize(folderPath: String): Long = {
    val path = Paths.get(folderPath)
    if (!Files.isDirectory(path)) {
      throw new IllegalArgumentException(s"$folderPath is not a valid directory.")
    }
    val size = Files.walk(path)
      .filter(p => p != path && Files.isRegularFile(p))
      .mapToLong(p => Files.size(p))
      .sum()

    size
  }
  println(getCurrentFolderSize("/home/avyuthan-shah/Desktop/Data/dataF"))
}
