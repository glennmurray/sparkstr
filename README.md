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


StreamSummer
------------

This job takes a stream of key-value pairs `(String, Double)` and accumulates
the sum of the values per key.

Run this in sbt with

    > run-main sparkstr.StreamSummer local[2] 1


StreamAverager
--------------

This job takes a stream of key-value pairs `(String, Double)` and gives the
average value over time for all the values for each key.

Run this in sbt with

    > run-main sparkstr.StreamAverager local[2] 1


StreamLargest
-------------

Find the current *k* largest (and *j* smallest) values in a stream.

Run this in sbt with

    > run-main sparkstr.StreamLargest local[2] 1

HyperLogLog
-----------

An implementation of the HyperLogLog algorithm by [Flajolet
et.al](http://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.142.9475). This
uses probabilistic techniques to estimate the size of data.  A good discussion
is at the [AK Tech Blog](http://blog.aggregateknowledge.com/2012/10/25/sketch-of-the-day-hyperloglog-cornerstone-of-a-big-data-infrastructure/),
which also sports a visualization tool.

Run this in sbt with

    > run-main sparkstr.HyperLogLog local[2] 1

The defaults are to create a single set with a million members.  It may be
worthwhile to use `local[4]` instead of `local[2]` if you have the available cores.
