package com.expedia.www


object StreamGenerator {

  val customerNames = Array("Arya", "Ned", "Catelyn", "Joffery", "Cersei", "Jamie", "Daenerys", "Tyrion", "Ser-Jonah", "Bran", "Faceless")
  val hotelNames = Array("King's-Landing", "Mereen", "Winterfell", "High-Garden", "Dorne", "Wall", "Riverunn", "Iron-Islands", "Braavos")
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("Usage: StreamGenerator <Num of Messages>")
      return
    }
    val numOfMessages = args(0).toInt
    val rnd = new scala.util.Random
    val impression = new KafkaProducer("impressions", true)
    val clicks = new KafkaProducer("clicks", true)
    for (i <- 1 to numOfMessages) {
      Thread.sleep(50)
      val customer = customerNames(rnd.nextInt(11))
      val hotel = hotelNames(rnd.nextInt(8))
      val time = System.currentTimeMillis()
      impression.run(customer + " " + hotel + " " + (time%100000).toString)
      if (rnd.nextInt(numOfMessages) < numOfMessages/20)
        clicks.run(customer + " " + hotel + " " + (time%100000 + rnd.nextInt(1000)).toString)
      else
        clicks.run(customer +  " " + hotel + " " + 0.toString)
    }
    //val consumerImp = new Consumer("impressions")
    //val consumerClk = new Consumer("clicks")
    //while(consumerImp.numOfMessages <= numOfMessages) {
     // consumerImp.doWork()
      //consumerClk.doWork()
    //}


  }
}

