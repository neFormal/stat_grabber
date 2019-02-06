name := "stat"

version := "1.0"

scalaVersion := "2.12.7"

lazy val akkaVersion = "2.5.19"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion,
  "com.typesafe.akka" %% "akka-actor-typed" % "2.5.19",
  "com.zaxxer" % "HikariCP" % "3.3.0",
  "org.postgresql" % "postgresql" % "42.2.5",
  "org.scalactic" %% "scalactic" % "3.0.5",
  "org.scalatest" %% "scalatest" % "3.0.5" % "test"
)
