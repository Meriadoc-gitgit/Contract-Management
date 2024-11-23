name := "Contract-Management"
version := "1.0"
scalaVersion := "2.12.15"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.1"
libraryDependencies += "com.beust" % "jcommander" % "1.48"
javacOptions ++= Seq("-source", "1.8", "-target", "1.8")
libraryDependencies += "com.databricks" % "spark-xml_2.12" % "0.14.0" // Use appropriate version
