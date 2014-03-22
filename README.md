Sparkstr
========

This project is composed of examples of 
[Spark Streaming](http://spark.incubator.apache.org/docs/latest/index.html)
jobs.  These are proofs of concepts.

Installation
------------

This assumes some knowledge of Java and Scala.  The code was developed on Linux
with Java 7.

Install [sbt](http://www.scala-sbt.org/) and
[Spark](https://spark.incubator.apache.org/).  The versions are given in the
`libraryDependencies` variable in the `project/build.sbt` file.

Clone this repository using a url on the right and run `sbt` in the sparkstr
directory.  At the sbt prompt, run `compile` and then one of the `run-main`
commands below.


Stream Summer
-------------

This very simple job takes a stream of key-value pairs `(String, Double)` and
accumulates the sum of the values per key.

Run this in sbt with

    > run-main sparkstr.StreamSummer local[2] 1


Largest and Smallest Values in a Stream
---------------------------------------

Find the current *k* largest and *j* smallest values in a stream.

Run this in sbt with

    > run-main sparkstr.StreamLargest local[2] 1


Stream Average
--------------

This job takes a stream of key-value pairs `(String, Double)` and gives the
average (or mean) per key.

Run this in sbt with

    > run-main sparkstr.StreamAverager local[2] 1


Stream Variance
---------------

This job finds the variance (or, if desired, the standard deviation) over time
for all the values for each key.  It calulates the running average (mean) along
the way in the same straight-forward way as the Stream Average job.

Run this in sbt with

    > run-main sparkstr.StreamVariance local[2] 1

I rolled my own implementations here , but note that the same implementations
seem to be done well (i.e., with attention paid to floating-point issues) in
Spark's built-in
[StatCounter](https://github.com/apache/incubator-spark/blob/master/core/src/main/scala/org/apache/spark/util/StatCounter.scala).
I recommend using that.


HyperLogLog Implementation
--------------------------

This is an implementation of the HyperLogLog algorithm by [Flajolet
et.al](http://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.142.9475). The
algorithm uses probabilistic techniques to estimate the size of data.  A good
discussion is at the [AK Tech
Blog](http://blog.aggregateknowledge.com/2012/10/25/sketch-of-the-day-hyperloglog-cornerstone-of-a-big-data-infrastructure/),
which also sports a visualization tool.

Run this in sbt with

    > run-main sparkstr.HyperLogLog local[2] 1

The defaults are to create a single set with a million members.  It might be
worthwhile to use `local[4]` instead of `local[2]` if you have the available
cores.

HyperLogLog is sometimes called a *cardinality estimator*.  Note that
"cardinality" is used to mean "set size" in the strict sense of set---no
repeated (duplicate) values.  It could be called a *number of distinct values
estimator*.  

