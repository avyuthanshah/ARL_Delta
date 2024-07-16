package example.Extra

import oshi.SystemInfo
import oshi.hardware.{CentralProcessor, GlobalMemory}
import scala.util.control.Breaks._

object resourceMonitor extends App {

  private val systemInfo = new SystemInfo()
  private val processor: CentralProcessor = systemInfo.getHardware.getProcessor
  private val memory: GlobalMemory = systemInfo.getHardware.getMemory

  private def getCpuUsage(): Double = {
    val loadTicks = processor.getSystemCpuLoadTicks
    Thread.sleep(500) // Sleep for 0.5 seconds
    val cpuLoad = processor.getSystemCpuLoadBetweenTicks(loadTicks)
    cpuLoad
  }

  private def getMemoryUsage(): Double = {
    val usedMemory = memory.getTotal - memory.getAvailable
    usedMemory.toDouble // Convert to MB
  }

  private def monitorInitial(getUsuage:()=> Double):Double={
    var avgVal = 0.0
    var cnt = 0
    val startTime = System.currentTimeMillis()

    while (System.currentTimeMillis() - startTime < 5000) { // Monitor Initial load before ingestion
      val currentVal=getUsuage()
      avgVal+=currentVal
      cnt += 1
      println(".")
      Thread.sleep(100)
    }
    println()
    avgVal/cnt
  }

  private def monitorUsage(getUsage: () => Double): Double = {
    var count = 0
    var totalUsage = 0.0

    breakable {
      while(status.readFile()){
        val currUsage = getUsage()
        totalUsage += currUsage
        count += 1

        if (!status.readFile()) { // Check the shared flag
          break
        }
        Thread.sleep(500) // Sleep for 0.5 seconds between checks
      }
    }

    totalUsage / count
  }

  println("Monitoring Initial Load")
  private val avgCpuInitial=monitorInitial(getCpuUsage)*100
  private val avgMemInitial=monitorInitial(getMemoryUsage)/1024/1024


//  // Wait for active ingestion indicated by flag
  println("Waiting for active status")
  do{
    println(status.readFile())
    println("Waiting for active status")
    Thread.sleep(500)
  }while (!status.readFile())

  println(status.readFile())
  // If ingestion is active, start monitoring CPU and memory usage
  private val cpuMonitorThread = new Thread(new Runnable {
    override def run(): Unit = {
      val avgCpuUsage = monitorUsage(getCpuUsage)
      println(s"Initial CPU Usage: ${avgCpuInitial} New CPU Usuage:${avgCpuUsage} Average CPU Usage: ${avgCpuUsage * 100 - avgCpuInitial}%")
    }
  })

  private val memoryMonitorThread = new Thread(new Runnable {
    override def run(): Unit = {
      val avgMemoryUsage = monitorUsage(getMemoryUsage)
      println(s"Initial Memory Usage: ${avgMemInitial} New Memory Usage: ${avgMemoryUsage} Average Memory Usage: ${(avgMemoryUsage/(1024.0 * 1024.0))-avgMemInitial} MB")
    }
  })

  cpuMonitorThread.start()
  memoryMonitorThread.start()

  cpuMonitorThread.join()
  memoryMonitorThread.join()

//  println("Monitoring Thread Usage")
//
//  private val cpuMonitorThread = new Thread(new Runnable {
//    override def run(): Unit = {
//      val avgCpuUsage = monitorInitial(getCpuUsage)
//      println(s" Average CPU Usage: ${avgCpuUsage * 100}%")
//    }
//  })
//
//  private val memoryMonitorThread = new Thread(new Runnable {
//    override def run(): Unit = {
//      val avgMemoryUsage = monitorInitial(getMemoryUsage)
//      println(s"Average Memory Usage: ${avgMemoryUsage/(1024.0 * 1024.0)} MB")
//    }
//  })
//
//  cpuMonitorThread.start()
//  memoryMonitorThread.start()
//
//  cpuMonitorThread.join()
//  memoryMonitorThread.join()

}
