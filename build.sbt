name := "twitter-streaming"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
   "org.apache.spark" %% "spark-core" % "2.1.0" % "provided",
   "org.apache.spark" %% "spark-streaming" % "2.1.0" % "provided",
   "org.apache.bahir" %% "spark-streaming-twitter" % "2.0.0",
//   "org.apache.spark" %% "spark-streaming-twitter" % "1.6.3",
   "org.apache.spark" %% "spark-sql" % "2.1.0" % "provided"
)

mergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$")      => MergeStrategy.discard
  case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
  case "reference.conf"                                    => MergeStrategy.concat
  case _                                                   => MergeStrategy.first
}

