import sbt._

class LomongoProject(info: ProjectInfo) extends DefaultProject(info) {

  override def repositories = Set(
    "jboss" at "http://repository.jboss.org/nexus/content/groups/public"
  )

  override def libraryDependencies = Set(
      "com.googlecode.scalaz" %% "scalaz-core" % "5.0"
    , "org.jboss.netty" % "netty" % "3.2.2.Final"
    , "joda-time" % "joda-time" % "1.6.1"
    , "commons-codec" % "commons-codec" % "1.4"
    , "org.scalatest" % "scalatest" % "1.2" % "test"
  )

  override def fork = forkRun

  override def compileOptions = super.compileOptions ++ compileOptions("-unchecked")
}
