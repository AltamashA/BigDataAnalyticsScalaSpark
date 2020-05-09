package stackoverflow

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.junit._
import org.junit.Assert.assertEquals
import java.io.File

object StackOverflowSuite {
  val conf: SparkConf = new SparkConf().setMaster("local").setAppName("StackOverflow")
  val sc: SparkContext = new SparkContext(conf)
}

class StackOverflowSuite {
  import StackOverflowSuite._


  lazy val testObject = new StackOverflow {
    override val langs =
      List(
        "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
        "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")
    override def langSpread = 50000
    override def kmeansKernels = 45
    override def kmeansEta: Double = 20.0D
    override def kmeansMaxIterations = 120
  }

  @Test def `testObject can be instantiated`: Unit = {
    val instantiatable = try {
      testObject
      true
    } catch {
      case _: Throwable => false
    }
    assert(instantiatable, "Can't instantiate a StackOverflow object")
  }

  @Test def `Grouping questions and answers together` {
    val postings = List(
      Posting(1, 1, None, None, 0, None),
      Posting(1, 2, None, None, 0, None),
      Posting(2, 3, None, Some(1), 2, None),
      Posting(2, 4, None, Some(1), 5, None),
      Posting(2, 5, None, Some(2), 12, None),
      Posting(1, 6, None, None, 0, None)
    )
    val rdd = sc.makeRDD(postings)
    val results = testObject.groupedPostings(rdd).collect()

    assert(results.size == 2)
    assert(results.contains(
      (1, Iterable(
        (Posting(1, 1, None, None, 0, None), Posting(2, 3, None, Some(1), 2, None)),
        (Posting(1, 1, None, None, 0, None), Posting(2, 4, None, Some(1), 5, None))
      ))
    ))
    assert(results.contains(
      (2, Iterable(
        (Posting(1, 2, None, None, 0, None), Posting(2, 5, None, Some(2), 12, None))
      ))
    ))
  }

  @Test def `Maximum scores among answers per question` {
    val groupedQuestions = Seq(
      (1, Iterable(
        (Posting(1, 1, None, None, 0, None), Posting(2, 3, None, Some(1), 2, None)),
        (Posting(1, 1, None, None, 0, None), Posting(2, 4, None, Some(1), 5, None))
      )),
      (2, Iterable(
        (Posting(1, 2, None, None, 0, None), Posting(2, 5, None, Some(2), 12, None))
      )),
      (3, Iterable(
        (Posting(1, 6, None, None, 0, None), Posting(2, 7, None, Some(3), 2, None)),
        (Posting(1, 6, None, None, 0, None), Posting(2, 8, None, Some(3), 19, None)),
        (Posting(1, 6, None, None, 0, None), Posting(2, 9, None, Some(3), 10, None))
      ))
    )
    val rdd = sc.makeRDD(groupedQuestions)
    val results = testObject.scoredPostings(rdd).collect()

    assert(results.size == 3)
    results.foreach(println)
    assert(results.contains( (Posting(1, 1, None, None, 0, None), 5) ))
    assert(results.contains( (Posting(1, 2, None, None, 0, None), 12) ))
    assert(results.contains( (Posting(1, 6, None, None, 0, None), 19) ))
  }

//  @Test def `Vectors for clustering` {
//    val questionsWithTopAnswer = List(
//      (Posting(1, 1, None, None, 0, Some("Java")), 14),
//      (Posting(1, 1, None, None, 0, None), 5),
//      (Posting(1, 1, None, None, 0, Some("Scala")), 25),
//      (Posting(1, 1, None, None, 0, Some("JavaScript")), 3)
//    )
//
//    val rdd = sc.makeRDD(questionsWithTopAnswer)
//
//    val result = testObject.vectorPostings(rdd).collect()
//    assert (result == Array((50000, 14), (500000, 25), (0, 3)))
//  }

  @Rule def individualTestTimeout = new org.junit.rules.Timeout(100 * 1000)
}
