name := "processor-scala"

version := "1.0"

scalaVersion := "2.11.7"

sbtVersion := "0.13.9"

resolvers ++= Seq("spray repo" at "http://repo.spray.io")

libraryDependencies ++= Seq(
  "org.scala-lang.modules" %% "scala-async" % "0.9.5",
  "net.databinder.dispatch" %% "dispatch-core" % "0.11.2"
)