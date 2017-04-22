  import org.apache.spark.SparkConf
  import org.apache.spark.SparkContext
  object telecomanalysis{
    def main(args:Array[String]){
    
  // Reading data from csv after initializing sparkContext 
      val sc = new SparkContext(new   SparkConf().setAppName("hello ").setMaster("local[2]"))
      val datafile = "/home/edureka/Downloads/CDR.csv"
      val text = sc.textFile(datafile)
  // defining a schema for the dataset 
    case class Call(visitor_locn: String, call_duration:
       Integer, phone_no: String, error_code: String)
  
   val calls = text
               .map(_.split(","))
               .map(p =>Call(p(0),p(1).toInt,p(2),p(3)))
  var result = calls
                   .map(x => (x.visitor_locn,1))
                   .reduceByKey(_+_)
                   .collect
                   .sortBy(_._2);
     
    println(result.reverse.take(10).mkString("\n")); 
    }
}

