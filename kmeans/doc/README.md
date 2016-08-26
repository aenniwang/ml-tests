## Kmeans ##

original Spark kmeans sample data format
<pre><code>
7 lines (6 sloc)  72 Bytes
0.0 0.0 0.0
0.1 0.1 0.1
0.2 0.2 0.2
9.0 9.0 9.0
9.1 9.1 9.1
9.2 9.2 9.2
</code></pre>
Spark Mllib processes the sample as
<pre><code>
val data = sc.textFile("data/mllib/kmeans_data.txt")
val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble))).cache()
</code></pre>