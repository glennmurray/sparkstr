Sparkstr
========

This project holds some examples of 
[Spark Streaming](http://spark.incubator.apache.org/docs/latest/index.html). 
These are proofs of concepts.


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

