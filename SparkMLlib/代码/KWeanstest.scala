package test
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
object KWeanstest {
  def main(args: Array[String]) {
    val conf = new
        SparkConf().setAppName("Spark MLlib Exercise:K-Means Clustering").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val rawTrainingData = sc.textFile("E:\\云计算\\SparkMLlib\\旅游\\data_train.txt")
    //处理待训练数据
    val parsedTrainingData = rawTrainingData.filter(!isColumnNameLine(_)).map(s=>(for{i<-1 to s.split(",").length-1}
          yield s.split(",")(i).toDouble)).map(_.toArray).map(Vectors.dense(_)).cache()
    //模型参数，分别为：聚类的个数、K-means 算法的迭代次数、K-means 算法 run 的次数
    val numClusters =5
    val numIterations = 30
    val runTimes =3
    var clusterIndex: Int = 0
    val clusters: KMeansModel =
      KMeans.train(parsedTrainingData, numClusters, numIterations, runTimes)
    //输出聚类结果
    println("Cluster Number:" + clusters.clusterCenters.length)
    println("Cluster Centers Information Overview:")
    clusters.clusterCenters.foreach(
      x => {
        println("Center Point of Cluster " + clusterIndex + ":")
        println(x)
        clusterIndex += 1
      })

    val rawTestData = sc.textFile("E:\\云计算\\SparkMLlib\\旅游\\data_test.txt")
    //处理测试数据
    val parsedTestData = rawTestData.map(s=>(for{i<-1 to s.split(",").length-1}
          yield s.split(",")(i).toDouble)).map(_.toArray).map(Vectors.dense(_))
    //根据聚类训练模型，判断测试数据属于哪一类并输出结果
    parsedTestData.collect().foreach(testDataLine => {
      val predictedClusterIndex:
        Int = clusters.predict(testDataLine)
      println("The data " + testDataLine.toString + " belongs to cluster " +
        predictedClusterIndex)
    })
    println("Spark MLlib K-means clustering test finished.")
  }
  //去表头
  private def
  isColumnNameLine(line: String): Boolean = {
    if (line != null &&
      line.contains("Channel")) true
    else false
  }
}
