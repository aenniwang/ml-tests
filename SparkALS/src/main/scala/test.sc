
val r= Array[String]("1920","32","1","klsd")
//r.length
/*
val rest = r match {
  case Array(user,item,rate)=>
    (user.toInt,item.toInt, rate.toDouble)}
*/
val b=r map {case(u,i,r,c) =>(c,r,u,i)}
b.foreach(println)




