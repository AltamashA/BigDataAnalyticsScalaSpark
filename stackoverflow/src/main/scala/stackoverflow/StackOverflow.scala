package stackoverflow

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import annotation.tailrec
import scala.reflect.ClassTag

/** A raw stackoverflow posting, either a question or an answer */
case class Posting(postingType: Int, id: Int, acceptedAnswer: Option[Int], parentId: Option[QID], score: Int, tags: Option[String]) extends Serializable


/** The main class */
object StackOverflow extends StackOverflow {

  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("StackOverflow")
  @transient lazy val sc: SparkContext = new SparkContext(conf)

  /** Main function */
  def main(args: Array[String]): Unit = {

    val lines   = sc.textFile("src/main/scala/stackoverflow/stackoverflow.csv")
    val raw     = rawPostings(lines)
    val grouped = groupedPostings(raw)
    val scored  = scoredPostings(grouped)
    val vectors = vectorPostings(scored)
   // assert(vectors.count() == 2121822, "Incorrect number of vectors: " + vectors.count())

    val means   = kmeans(sampleVectors(vectors), vectors, debug = true)
    val results = clusterResults(means, vectors)
    printResults(results)
  }
}

/** The parsing and kmeans methods */
class StackOverflow extends StackOverflowInterface with Serializable {

  /** Languages */
  val langs =
    List(
      "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
      "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

  /** K-means parameter: How "far apart" languages should be for the kmeans algorithm? */
  def langSpread = 50000
  assert(langSpread > 0, "If langSpread is zero we can't recover the language from the input data!")

  /** K-means parameter: Number of clusters */
  def kmeansKernels = 45

  /** K-means parameter: Convergence criteria */
  def kmeansEta: Double = 20.0D

  /** K-means parameter: Maximum iterations */
  def kmeansMaxIterations = 120


  //
  //
  // Parsing utilities:
  //
  //

  /** Load postings from the given file */
  def rawPostings(lines: RDD[String]): RDD[Posting] =
    lines.map(line => {
      val arr = line.split(",")
      Posting(postingType =    arr(0).toInt,
              id =             arr(1).toInt,
              acceptedAnswer = if (arr(2) == "") None else Some(arr(2).toInt),
              parentId =       if (arr(3) == "") None else Some(arr(3).toInt),
              score =          arr(4).toInt,
              tags =           if (arr.length >= 6) Some(arr(5).intern()) else None)
    })


  /** Group the questions and answers together
    * In the raw variable we have simple postings, either questions or answers, but in order to use the data we
    * need to assemble them together. Questions are identified using a postTypeId == 1. Answers to a question
    * with id == QID have (a) postTypeId == 2 and (b) parentId == QID.
    *
    * Ideally, we want to obtain an RDD with the pairs of (Question, Iterable[Answer]). However, grouping on the
    * question directly is expensive (can you imagine why?), so a better alternative is to match on the QID, thus
    * producing an RDD[(QID, Iterable[(Question, Answer))].
    *
    * To obtain this, in the groupedPostings method, first filter the questions and answers separately and then
    * prepare them for a join operation by extracting the QID value in the first element of a tuple. Then, use one
    * of the join operations (which one?) to obtain an RDD[(QID, (Question, Answer))]. Then, the last step is to obtain
    * an RDD[(QID, Iterable[(Question, Answer)])]. How can you do that, what method do you use to group by the key of a
    * pair RDD?
    *
    * Finally, in the description we used QID, Question and Answer types, which we've defined as type aliases
    * for Postings and Ints. The full list of type aliases is available in package.scala:*/
  def groupedPostings(postings: RDD[Posting]): RDD[(QID, Iterable[(Question, Answer)])] = {
    val questions = postings.filter(post => (post.postingType == 1)).map(post => (post.id , post))
    val answers = postings.filter(post => (post.postingType == 2)).map(post =>(post.parentId.get,post))
    questions.join(answers).persist().groupByKey()

  }


  /** Compute the maximum score for each posting
    * Computing Scores
    * Second, implement the scoredPostings method, which should return an RDD containing pairs of (a)
    * questions and (b) the score of the answer with the highest score (note: this does not have to be the answer
    * marked as acceptedAnswer!). The type of this scored RDD is:
    *
    * 
    * For example, the scored RDD should contain the following tuples:
    *
    * 
    * Hint: use the provided answerHighScore given in scoredPostings.
    *
    * Creating vectors for clustering
    * Next, we prepare the input for the clustering algorithm. For this, we transform the scored RDD into a vectors
    * RDD containing the vectors to be clustered. In our case, the vectors should be pairs with two components
    * (in the listed order!):
    *
    * Index of the language (in the langs list) multiplied by the langSpread factor.
    * The highest answer score (computed above).
    * The langSpread factor is provided (set to 50000). Basically, it makes sure posts about different
    * programming languages have at least distance 50000 using the distance measure provided by the euclideanDist
    * function. You will learn later what this distance means and why it is set to this value.
    *
    * The type of the vectors RDD is as follows:
    *
    * 
    * For example, the vectors RDD should contain the following tuples:
    *
    * 
    * Implement this functionality in method vectorPostings by using the given firstLangInTag helper method.
    *
    * (Idea for test: scored RDD should have 2121822 entries)*/
  def scoredPostings(grouped: RDD[(QID, Iterable[(Question, Answer)])]): RDD[(Question, HighScore)] = {

    def answerHighScore(as: Array[Answer]): HighScore = {
      var highScore = 0
          var i = 0
          while (i < as.length) {
            val score = as(i).score
                if (score > highScore)
                  highScore = score
                  i += 1
          }
      highScore
    }

    grouped.map(group => (group._2.head._1, answerHighScore(group._2.map(_._2).toArray)))
  }


  /** Compute the vectors for the kmeans */
  def vectorPostings(scored: RDD[(Question, HighScore)]): RDD[(LangIndex, HighScore)] = {
    /** Return optional index of first language that occurs in `tags`. */
    def firstLangInTag(tag: Option[String], ls: List[String]): Option[Int] = {
      if (tag.isEmpty) None
      else if (ls.isEmpty) None
      else if (tag.get == ls.head) Some(0) // index: 0
      else {
        val tmp = firstLangInTag(tag, ls.tail)
        tmp match {
          case None => None
          case Some(i) => Some(i + 1) // index i in ls.tail => index i+1
        }
      }
    }

    scored.map(score => (firstLangInTag(score._1.tags,langs).get * langSpread ,score._2))
      .partitionBy(new HashPartitioner(langs.length)).cache()
  }


  /** Sample the vectors */
  def sampleVectors(vectors: RDD[(LangIndex, HighScore)]): Array[(Int, Int)] = {

    assert(kmeansKernels % langs.length == 0, "kmeansKernels should be a multiple of the number of languages studied.")
    val perLang = kmeansKernels / langs.length

    // http://en.wikipedia.org/wiki/Reservoir_sampling
    def reservoirSampling(lang: Int, iter: Iterator[Int], size: Int): Array[Int] = {
      val res = new Array[Int](size)
      val rnd = new util.Random(lang)

      for (i <- 0 until size) {
        assert(iter.hasNext, s"iterator must have at least $size elements")
        res(i) = iter.next
      }

      var i = size.toLong
      while (iter.hasNext) {
        val elt = iter.next
        val j = math.abs(rnd.nextLong) % i
        if (j < size)
          res(j.toInt) = elt
        i += 1
      }

      res
    }

    val res =
      if (langSpread < 500)
        // sample the space regardless of the language
        vectors.takeSample(false, kmeansKernels, 42)
      else
        // sample the space uniformly from each language partition
        vectors.groupByKey.flatMap({
          case (lang, vectors) => reservoirSampling(lang, vectors.toIterator, perLang).map((lang, _))
        }).collect()

    assert(res.length == kmeansKernels, res.length)
    res
  }


  //
  //
  //  Kmeans method:
  //
  //

  /** Main kmeans computation
    * Based on these initial means, and the provided variables converged method,
    * implement the K-means algorithm by iteratively:
    *
    * pairing each vector with the index of the closest mean (its cluster);
    * computing the new means by averaging the values of each cluster.*/
  @tailrec final def kmeans(means: Array[(Int, Int)], vectors: RDD[(Int, Int)], iter: Int = 1, debug: Boolean = false): Array[(Int, Int)] = {
    val newMeans = means.clone() // you need to compute newMeans

    // TODO: Fill in the newMeans array
   val meansUpdated = vectors.map(v=> (findClosest(v,means),v)).groupByKey().mapValues(vs => averageVectors(vs)).collect();
    for((k,meansU)<- meansUpdated){
      newMeans.update(k,meansU)
    }
    val distance = euclideanDistance(means, newMeans)

    if (debug) {
      println(s"""Iteration: $iter
                 |  * current distance: $distance
                 |  * desired distance: $kmeansEta
                 |  * means:""".stripMargin)
      for (idx <- 0 until kmeansKernels)
      println(f"   ${means(idx).toString}%20s ==> ${newMeans(idx).toString}%20s  " +
              f"  distance: ${euclideanDistance(means(idx), newMeans(idx))}%8.0f")
    }

    if (converged(distance))
      newMeans
    else if (iter < kmeansMaxIterations)
      kmeans(newMeans, vectors, iter + 1, debug)
    else {
      if (debug) {
        println("Reached max iterations!")
      }
      newMeans
    }
  }




  //
  //
  //  Kmeans utilities:
  //
  //

  /** Decide whether the kmeans clustering converged */
  def converged(distance: Double) =
    distance < kmeansEta


  /** Return the euclidean distance between two points */
  def euclideanDistance(v1: (Int, Int), v2: (Int, Int)): Double = {
    val part1 = (v1._1 - v2._1).toDouble * (v1._1 - v2._1)
    val part2 = (v1._2 - v2._2).toDouble * (v1._2 - v2._2)
    part1 + part2
  }

  /** Return the euclidean distance between two points */
  def euclideanDistance(a1: Array[(Int, Int)], a2: Array[(Int, Int)]): Double = {
    assert(a1.length == a2.length)
    var sum = 0d
    var idx = 0
    while(idx < a1.length) {
      sum += euclideanDistance(a1(idx), a2(idx))
      idx += 1
    }
    sum
  }

  /** Return the closest point */
  def findClosest(p: (Int, Int), centers: Array[(Int, Int)]): Int = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity
    for (i <- 0 until centers.length) {
      val tempDist = euclideanDistance(p, centers(i))
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }
    bestIndex
  }


  /** Average the vectors */
  def averageVectors(ps: Iterable[(Int, Int)]): (Int, Int) = {
    val iter = ps.iterator
    var count = 0
    var comp1: Long = 0
    var comp2: Long = 0
    while (iter.hasNext) {
      val item = iter.next
      comp1 += item._1
      comp2 += item._2
      count += 1
    }
    ((comp1 / count).toInt, (comp2 / count).toInt)
  }




  //
  //
  //  Displaying results:
  //
  //
  def clusterResults(means: Array[(Int, Int)], vectors: RDD[(LangIndex, HighScore)]): Array[(String, Double, Int, Int)] = {
    val closest = vectors.map(p => (findClosest(p, means), p))
    val closestGrouped = closest.groupByKey()

    val median = closestGrouped.mapValues { vs =>
      val maxLang = vs.groupBy(vec => (vec._1)).mapValues(_.size).maxBy(_._2)._1/langSpread
      val langLabel: String   = langs(maxLang)

      val langPercent: Double =  vs.count(v => (v._1/langSpread )== langs.indexOf(langLabel))*100/vs.size
      val clusterSize: Int    = vs.size
      val ln = vs.map(v => v._2).toArray.length
//      val arr = vs.map(v => v._2).toArray.sortWith(_<_)
      val (low,high ) = vs.map(v => v._2).toArray.sortWith(_<_).splitAt(ln/2)
      val medianScore: Int    = if(ln%2==0 ){
        (low.last + high.head )/2
      }else{
        high.head
      }

      (langLabel, langPercent, clusterSize, medianScore)
    }

    median.collect().map(_._2).sortBy(_._4)
  }

  def printResults(results: Array[(String, Double, Int, Int)]): Unit = {
    println("Resulting clusters:")
    println("  Score  Dominant language (%percent)  Questions")
    println("================================================")
    for ((lang, percent, size, score) <- results)
      println(f"${score}%7d  ${lang}%-17s (${percent}%-5.1f%%)      ${size}%7d")
  }
}
