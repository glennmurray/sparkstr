name:="sparkstr"

scalaVersion :="2.10.4"

version :="0.2"

logLevel := Level.Warn

logLevel in Test := Level.Info

// For "%%" see
// http://www.scala-sbt.org/release/docs/Getting-Started/Library-Dependencies
libraryDependencies ++= {
  Seq(
    "org.apache.spark" %% "spark-core" % "1.2.1",
    "org.apache.spark" %% "spark-streaming" % "1.2.1",
    //
    // Linear algebra
    "org.scalanlp" %% "breeze" % "0.10",
    // native libraries are not included by default. add this if you want them.
    // native libraries greatly improve performance, but increase jar sizes.
    //"org.scalanlp" %% "breeze-natives" % "0.10",
    //
    // Testing
    "org.scalatest" %% "scalatest" % "2.2.4" % "test",
    "org.scalacheck" %% "scalacheck" % "1.12.2" % "test",
    "junit" % "junit" % "4.8.1" % "test"
  )
}

// For ScalaTest, -o options.
testOptions in Test += Tests.Argument("-oDS")

// Assembly.  Making a fat jar.
assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*)            => MergeStrategy.first
  case PathList("javax", "activation", xs @ _*)         => MergeStrategy.first
  case PathList("org.eclipse.jetty.orbit", xs @ _*)     => MergeStrategy.first
  case PathList("com", "esotericsoftware", xs @ _*)     => MergeStrategy.first
  case PathList("org", "apache", "commons", xs @ _*)    => MergeStrategy.first
  case PathList("org", "apache", "hadoop", "yarn", xs @ _*) => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".html"    => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith "mailcap"  => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith "mimetypes.default" => MergeStrategy.first
  case "plugin.properties"     => MergeStrategy.discard   //filterDistinctLines
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

// To skip the test during assembly, uncomment.
//test in assembly := {}
