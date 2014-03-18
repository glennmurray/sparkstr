import AssemblyKeys._ // put this at the top of the file

name:="sparkstr"

scalaVersion :="2.10.3"

version :="0.1"

logLevel := Level.Warn

logLevel in Test := Level.Info

resolvers ++= Seq(
  "Akka Repository" at "http://repo.akka.io/releases/",
  "Spray Repository" at "http://repo.spray.cc/",
  "snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
  "releases" at "http://oss.sonatype.org/content/repositories/releases"
  )

libraryDependencies ++= {
  Seq(
    //"org.scalatest" %% "scalatest" % "1.9.1" % "test",
    "org.scalatest" % "scalatest_2.10" % "2.1.0" % "test",
    "org.apache.spark" %% "spark-core" % "0.9.0-incubating" % "provided",
    "org.apache.spark" %% "spark-streaming" % "0.9.0-incubating" % "provided",
    "org.slf4j" % "slf4j-log4j12" % "1.7.2",
    "junit" % "junit" % "4.8.1" % "test"
  )
}

runMain in Compile <<= Defaults.runMainTask(fullClasspath in Compile, runner in (Compile, run))

run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))

testOptions in Test += Tests.Argument("-oDS")

assemblySettings

//mergeStrategy at https://groups.google.com/forum/#!topic/spark-users/pHaF01sPwBo

assembleArtifact in packageScala := false

test in assembly := {}
