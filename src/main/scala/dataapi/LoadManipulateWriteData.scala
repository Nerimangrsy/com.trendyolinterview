package dataapi


import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala._
import org.apache.flink.util.Collector


object LoadManipulateWriteData extends App {

  val env =  ExecutionEnvironment.getExecutionEnvironment

 //Define Event schema
  case class event (date: Int,
                    productId: Int,
                    eventName: String,
                    userId: Int
                   )

  // Create dataset from file
  val eventData = env.readCsvFile[event]("C:/Users/NERIMAN.GURSOY/Desktop/Case_20210304/case.csv", fieldDelimiter = "|", ignoreFirstLine = true)



  // 1.Unique product view counts by ProductId
  // Eg: 113|2 114|1 123|1

  //eventData.map(x => (x.productId, 1)).groupBy(0).sum(1).print()


  val resultOfCount = eventData.map(x => (x.productId, 1)).groupBy(0).sum(1)

  resultOfCount.rebalance().writeAsCsv("C:/Users/NERIMAN.GURSOY/Desktop/Case_20210304/Question1.csv", "\n", fieldDelimiter="|",
    org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE).setParallelism(1)




  //2. Unique event counts
  //Eg: add|3 click|4 remove|33 view|77

  //eventData.map(x => (x.eventName, 1)).groupBy(0).sum(field = 1).print()


  val resultOfEvent = eventData.map(x => (x.eventName, 1)).groupBy(0).sum(field = 1)

  resultOfEvent.rebalance().writeAsCsv("C:/Users/NERIMAN.GURSOY/Desktop/Case_20210304/Question2.csv", "\n", fieldDelimiter="|",
    org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE).setParallelism(1)




  //3. Top 5 users who fulfilled all the events (view, add, remove, click)
  //Eg: 21 18


  val resultOfTop5User = eventData.map(x => (x.userId, x.eventName)).groupBy(0).combineGroup((in, out: Collector[(Int, Int, Int)]) => {
      var key: Int = 0
      var events = Seq[String]()
      for (x <- in) {
        key = x._1
        events = events :+ x._2
      }
      out.collect((key, events.size, events.toSet.size))
    })
   .sortPartition(1, Order.DESCENDING)
   .filter(entry => entry._3 == 4)
   .first(5)
   .map(entry => entry._1)

  resultOfTop5User.rebalance().writeAsText("C:/Users/NERIMAN.GURSOY/Desktop/Case_20210304/Question3.csv",
    org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE).setParallelism(1)





  //4. All events of #UserId: 47
  //Eg: add|12 Remove|54


  val resultOf47 = eventData.filter(x => x.userId == 47).map(x => (x.eventName,1)).groupBy(0).sum(1)

  resultOf47.rebalance().writeAsCsv("C:/Users/NERIMAN.GURSOY/Desktop/Case_20210304/Question4.csv", "\n", fieldDelimiter="|",
    org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE).setParallelism(1)




  //5. Product views of #UserId: 47
  //Eg: 32 44 39 123


  val resultOf47ProductView = eventData.filter(x => x.eventName == "view" & x.userId == 47).map(x => (x.productId))

  resultOf47ProductView.rebalance().writeAsText("C:/Users/NERIMAN.GURSOY/Desktop/Case_20210304/Question5.csv",
    org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE).setParallelism(1)
  env.execute()


}
