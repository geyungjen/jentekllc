// this file was written for spark 2.0.0 and scala 2.12.10
// George Jen
// Jen Tek LLC

version := "1"
name := "JentekLLC-spark-streaming-from-Twitter"
scalaVersion := "2.12.10"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.0-preview" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.0.0-preview" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.0-preview" % "provided"
libraryDependencies += "org.twitter4j" % "twitter4j-core" % "4.0.4"
libraryDependencies += "org.twitter4j" % "twitter4j-stream" % "4.0.4"
libraryDependencies += "org.apache.bahir" %% "spark-streaming-twitter" % "2.4.0"

assemblyMergeStrategy in assembly := {
  case PathList("org","aopalliance", xs @ _*) => MergeStrategy.last
  case PathList("javax", "inject", xs @ _*) => MergeStrategy.last
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("com", "google", xs @ _*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
  case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
