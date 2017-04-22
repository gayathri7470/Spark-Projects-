  import org.apache.spark.SparkConf
  import org.apache.spark.SparkContext
  
  object hello{
    def main(args:Array[String]){
    
  // Reading data from csv after initializing sparkContext 
      val sc = new SparkContext(new   SparkConf().setAppName("hello ").setMaster("local[2]"))
      val datafile = "/home/edureka/Downloads/CDR.csv"
      val text = sc.textFile(datafile)
    
       
  // defining a schema for the dataset 
    
    case class Call(visitor_locn: String, call_duration:
       Integer, phone_no: String, error_code: String)
  
   val calls = text.map(_.split(",")).map(p =>
       Call(p(0),p(1).toInt,p(2),p(3)))
      
     println(calls.count());
      calls.foreach { x =>  println(x)}
    
       var result = calls.map(x => (x.visitor_locn,1)).reduceByKey(_+_).collect.sortBy(_._2);
      // Counting the number of errors are storing the result 
     var result2 = calls.map(x => (x.error_code,1)).reduceByKey(_+_).collect.sortBy(_._2);

}
}

