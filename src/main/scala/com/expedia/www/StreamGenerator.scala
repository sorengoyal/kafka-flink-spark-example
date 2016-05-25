package com.expedia.www


object StreamGenerator {

  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("Usage: StreamGenerator <Num of Messages>")
      return
    }
    val numOfMessages = args(0).toInt
    val rnd = new scala.util.Random
    val impression = new Producer("impressions", true)
    val clicks = new Producer("clicks", true)
    for (i <- 1 to numOfMessages) {
      Thread.sleep(100)
      val customerId = rnd.nextInt(100)
      val hotelId = rnd.nextInt(100)
      val time = System.currentTimeMillis()
      impression.run(customerId.toString + " " + hotelId.toString + " " + time.toString)
      if (rnd.nextInt(100) < numOfMessages/10)
        clicks.run(customerId.toString + " " + hotelId.toString + " " + (time + rnd.nextInt(1000)).toString)
    }
    val consumerImp = new Consumer("impressions")
    val consumerClk = new Consumer("clicks")
    while(consumerImp.numOfMessages <= numOfMessages) {
      consumerImp.doWork()
      consumerClk.doWork()
    }


  }
}

