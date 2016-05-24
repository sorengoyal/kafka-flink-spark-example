package com.expedia.www


object StreamGenerator {

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("Usage: StreamGenerator <topic> <Num of Messages>")
      return
    }
    val topic = args(0)
    val numOfMessages = args(1).toInt
    val rnd = new scala.util.Random
    val impression = new Producer("impressions", true)
    val clicks = new Producer("clicks", true)
    for (i <- 1 to numOfMessages) {
      val customerId = rnd.nextInt(100)
      val hotelId = rnd.nextInt(100)
      val time = System.currentTimeMillis()
      impression.send(customerId.toString + " " + hotelId.toString + " " + time.toString)
      if (rnd.nextInt(100) < 10)
        clicks.send(customerId.toString + " " + hotelId.toString + " " + (time + rnd.nextInt(1000)).toString)
    }
  }
}

