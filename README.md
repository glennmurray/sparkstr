Sparkstr
========

This project holds some examples of 
[Spark Streaming](http://spark.incubator.apache.org/docs/latest/index.html). 
These are proofs of concepts.

Installation
------------

Install [sbt](http://www.scala-sbt.org/).  This was developed with sbt version
12.4.  

Install [Spark](https://spark.incubator.apache.org/).  This was developed with
version 0.8.1.

Clone this repository using a url on the right and run 'sbt' in the sparkstr
directory.  At the sbt prompt, run 'compile' and then one of the 'run-main'
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

