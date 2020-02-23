package test
import org.apache.spark.mllib.classification.{NaiveBayes,NaiveBayesModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkContext,SparkConf}

object MyNaiveBayes {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("NaiveBayes")
    val sc = new SparkContext(conf)
    val path = "E:\\云计算\\SparkMLlib\\NaiveBayesPredict\\data_2.txt"
    val data = sc.textFile(path)
    val parsedData =data.map {
      line =>
        val parts =line.split(",:,")
        LabeledPoint(parts(1).toDouble,Vectors.dense(parts(0).split(',').map(_.toDouble)))
    }
    //样本划分train和test数据样本60%用于train
    val splits = parsedData.randomSplit(Array(0.9,0.1),seed = 11L)
    //
    //该函数根据weights权重，将一个RDD切分成多个RDD。
    // 该权重参数为一个Double数组第二个参数为random的种子，基本可忽略。
    //
    val training =splits(0)
    val test =splits(1)
    test.foreach(println)//输出数据
    //获得训练模型,第一个参数为数据，第二个参数为平滑参数，默认为1，可改变
    val model =NaiveBayes.train(training,lambda = 1.0)
    //对测试样本进行测试
    //对模型进行准确度分析
    val predictionAndLabel= test.map(p => (model.predict(p.features),p.label))
    predictionAndLabel.foreach(println)//以(预测值，实际值)得形式输出结果
    val accuracy =1.0 *predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()
    //打印一个预测值
    println("NaiveBayes精度----->" + accuracy)

    //保存model
    val ModelPath = "E:\\云计算\\SparkMLlib\\NaiveBayesPredict\\NaiveBayes_model.obj"
    model.save(sc,ModelPath)
  }
}