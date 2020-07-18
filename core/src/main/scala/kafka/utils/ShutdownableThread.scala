package kafka.utils

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.CountDownLatch

abstract class ShutdownableThread(val name: String, val isInterruptible: Boolean = true)
        extends Thread(name) with Logging {
  this.setDaemon(false)
  this.logIdent = "[" + name + "], "
  val isRunning: AtomicBoolean = new AtomicBoolean(true)
  private val shutdownLatch = new CountDownLatch(1)

  def shutdown() = {
    initiateShutdown()
    awaitShutdown()
  }

  def initiateShutdown(): Boolean = {
    if(isRunning.compareAndSet(true, false)) {
      info("Shutting down")
      isRunning.set(false)
      if (isInterruptible)
        interrupt()
      true
    } else
      false
  }

    /**
   * After calling initiateShutdown(), use this API to wait until the shutdown is complete
   */
  def awaitShutdown(): Unit = {
    shutdownLatch.await()
    info("Shutdown completed")
  }

  /**
   * This method is repeatedly invoked until the thread shuts down or this method throws an exception
   */
  def doWork(): Unit

  override def run(): Unit = {
    info("Starting ")
    try{
      while(isRunning.get()){
        doWork()
      }
    } catch{
      case e: Throwable =>
        if(isRunning.get())
          error("Error due to ", e)
    }
    shutdownLatch.countDown()
    info("Stopped ")
  }
}