import org.apache.spark.mllib.recommendation.{ALS=>mllibALS, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by david on 8/15/16.
  */
object ALS {
  private var datRating="/Spark/ALS/MoviveLens/6M/data/ratings.dat"
  private var datMov="/Spark/ALS/MoviveLens/6M/data/movies.dat"

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("SparkALS-MovieLen")
    val sc = new SparkContext(conf)

    val rddRatings=sc.textFile(datRating)
    // split each element
    // movie format
    // "1991::Child's Play (1988)::Horror"
    // rating format
    // "UserID::ItemID::rating::timestamp"
    def getRatings(r:String):(Int,Int,Double)={
      val vals=r.split("::")
      (vals(0).toInt, vals(1).toInt, vals(2).toDouble)
    }
    val ratings = rddRatings.map(r=>getRatings(r)).map({
      case (user,item,rate)=>Rating(user,item,rate)})

    val model = mllibALS.train(ratings,10,10,0.01)
    val user=ratings.first().user

    def getAllMovies()={
      val rddMov=sc.textFile(datMov)
      val movDesc=rddMov.map(m=> {
        val items = m.split("::")
        (items(0), items(1))
      })
      movDesc
    }

    val movieIDs=getAllMovies().map(p=>p._1).collect().map(_.toInt)
    println(s"Predict for user $user")
    movieIDs.foreach(movie=>{
      val result=model.predict(user,movie)
    })
  }

  // list source data information
  // UserID count, non-overlap count
  // MoiveID count, non-overlap count
  // non-zero ratings
  // How many movies for one user that rated in average.
  def showDataStatistic():Unit={

  }

  //
  def trainSparkExplicit(rating:RDD[(Int, Int, Double)])={

  }
}
