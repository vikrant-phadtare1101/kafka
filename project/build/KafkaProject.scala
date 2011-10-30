/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import sbt._
import scala.xml.{Node, Elem, NodeSeq}
import scala.xml.transform.{RewriteRule, RuleTransformer}

class KafkaProject(info: ProjectInfo) extends ParentProject(info) with IdeaProject {
  lazy val core = project("core", "core-kafka", new CoreKafkaProject(_))
  lazy val examples = project("examples", "java-examples", new KafkaExamplesProject(_), core)
  lazy val contrib = project("contrib", "contrib", new ContribProject(_))

  lazy val releaseZipTask = core.packageDistTask

  val releaseZipDescription = "Compiles every sub project, runs unit tests, creates a deployable release zip file with dependencies, config, and scripts."
  lazy val releaseZip = releaseZipTask dependsOn(core.corePackageAction, core.test, examples.examplesPackageAction,
    contrib.producerPackageAction, contrib.consumerPackageAction) describedAs releaseZipDescription

  // Not sure why rat does not get pulled from a Maven repo automatically.
  val rat = "org.apache.rat" % "apache-rat-project" % "0.7"

  val runRatDescription = "Runs Apache rat on Kafka"
  lazy val runRatTask = task {
    val rat = "org.apache.rat" % "apache-rat-project" % "0.7"
    Runtime.getRuntime().exec("bin/run-rat.sh")
    None
  } describedAs runRatDescription


  class CoreKafkaProject(info: ProjectInfo) extends DefaultProject(info)
     with IdeaProject with CoreDependencies with TestDependencies {
   val corePackageAction = packageAllAction

  //The issue is going from log4j 1.2.14 to 1.2.15, the developers added some features which required
  // some dependencies on various sun and javax packages.
   override def ivyXML =
    <dependencies>
      <exclude module="javax"/>
      <exclude module="jmxri"/>
      <exclude module="jmxtools"/>
      <exclude module="mail"/>
      <exclude module="jms"/>
      <dependency org="org.apache.zookeeper" name="zookeeper" rev="3.3.3">
        <exclude module="log4j"/>
        <exclude module="jline"/>
      </dependency>
    </dependencies>

    def zkClientDep =
      <dependency>
       <groupId>zkclient</groupId>
       <artifactId>zkclient</artifactId>
       <version>20110412</version>
       <scope>compile</scope>
       </dependency>

  object ZkClientDepAdder extends RuleTransformer(new RewriteRule() {
      override def transform(node: Node): Seq[Node] = node match {
        case Elem(prefix, "dependencies", attribs, scope, deps @ _*) => {
          Elem(prefix, "dependencies", attribs, scope, deps ++ zkClientDep :_*)
        }
        case other => other
      }
    })

    override def pomPostProcess(pom: Node): Node = {
      ZkClientDepAdder(pom)
    }

    override def repositories = Set(ScalaToolsSnapshots, "JBoss Maven 2 Repository" at "http://repository.jboss.com/maven2",
      "Oracle Maven 2 Repository" at "http://download.oracle.com/maven", "maven.org" at "http://repo2.maven.org/maven2/")

    override def artifactID = "kafka"
    override def filterScalaJars = false

    // build the executable jar's classpath.
    // (why is it necessary to explicitly remove the target/{classes,resources} paths? hm.)
    def dependentJars = {
      val jars =
      publicClasspath +++ mainDependencies.scalaJars --- mainCompilePath --- mainResourcesOutputPath
      if (jars.get.find { jar => jar.name.startsWith("scala-library-") }.isDefined) {
        // workaround bug in sbt: if the compiler is explicitly included, don't include 2 versions
        // of the library.
        jars --- jars.filter { jar =>
          jar.absolutePath.contains("/boot/") && jar.name == "scala-library.jar"
        }
      } else {
        jars
      }
    }

    def dependentJarNames = dependentJars.getFiles.map(_.getName).filter(_.endsWith(".jar"))
    override def manifestClassPath = Some(dependentJarNames.map { "libs/" + _ }.mkString(" "))

    def distName = (artifactID + "-" + projectVersion.value)
    def distPath = "dist" / distName ##

    def configPath = "config" ##
    def configOutputPath = distPath / "config"

    def binPath = "bin" ##
    def binOutputPath = distPath / "bin"

    def distZipName = {
      "%s-%s.zip".format(artifactID, projectVersion.value)
    }

    lazy val packageDistTask = task {
      distPath.asFile.mkdirs()
      (distPath / "libs").asFile.mkdirs()
      binOutputPath.asFile.mkdirs()
      configOutputPath.asFile.mkdirs()

      FileUtilities.copyFlat(List(jarPath), distPath, log).left.toOption orElse
              FileUtilities.copyFlat(dependentJars.get, distPath / "libs", log).left.toOption orElse
              FileUtilities.copy((configPath ***).get, configOutputPath, log).left.toOption orElse
              FileUtilities.copy((binPath ***).get, binOutputPath, log).left.toOption orElse
              FileUtilities.zip((("dist" / distName) ##).get, "dist" / distZipName, true, log)
      None
    }

    val PackageDistDescription = "Creates a deployable zip file with dependencies, config, and scripts."
    lazy val packageDist = packageDistTask dependsOn(`package`, `test`) describedAs PackageDistDescription

    val cleanDist = cleanTask("dist" ##) describedAs("Erase any packaged distributions.")
    override def cleanAction = super.cleanAction dependsOn(cleanDist)

    override def javaCompileOptions = super.javaCompileOptions ++
      List(JavaCompileOption("-source"), JavaCompileOption("1.5"))

    override def packageAction = super.packageAction dependsOn (testCompileAction)

  }

  class KafkaExamplesProject(info: ProjectInfo) extends DefaultProject(info)
     with IdeaProject
     with CoreDependencies {
    val examplesPackageAction = packageAllAction
    val dependsOnCore = core
  //The issue is going from log4j 1.2.14 to 1.2.15, the developers added some features which required
  // some dependencies on various sun and javax packages.
   override def ivyXML =
    <dependencies>
      <exclude module="javax"/>
      <exclude module="jmxri"/>
      <exclude module="jmxtools"/>
      <exclude module="mail"/>
      <exclude module="jms"/>
    </dependencies>

    override def artifactID = "kafka-java-examples"
    override def filterScalaJars = false
    override def javaCompileOptions = super.javaCompileOptions ++
      List(JavaCompileOption("-Xlint:unchecked"))
  }

  class ContribProject(info: ProjectInfo) extends ParentProject(info) with IdeaProject {
    lazy val hadoopProducer = project("hadoop-producer", "hadoop producer",
                                      new HadoopProducerProject(_), core)
    lazy val hadoopConsumer = project("hadoop-consumer", "hadoop consumer",
                                      new HadoopConsumerProject(_), core)

    val producerPackageAction = hadoopProducer.producerPackageAction
    val consumerPackageAction = hadoopConsumer.consumerPackageAction

    class HadoopProducerProject(info: ProjectInfo) extends DefaultProject(info)
      with IdeaProject
      with CoreDependencies {
      val producerPackageAction = packageAllAction
      override def ivyXML =
       <dependencies>
         <exclude module="netty"/>
           <exclude module="javax"/>
           <exclude module="jmxri"/>
           <exclude module="jmxtools"/>
           <exclude module="mail"/>
           <exclude module="jms"/>
       </dependencies>

      val avro = "org.apache.avro" % "avro" % "1.4.1"
      val jacksonCore = "org.codehaus.jackson" % "jackson-core-asl" % "1.5.5"
      val jacksonMapper = "org.codehaus.jackson" % "jackson-mapper-asl" % "1.5.5"
    }

    class HadoopConsumerProject(info: ProjectInfo) extends DefaultProject(info)
      with IdeaProject
      with CoreDependencies {
      val consumerPackageAction = packageAllAction
      override def ivyXML =
       <dependencies>
         <exclude module="netty"/>
           <exclude module="javax"/>
           <exclude module="jmxri"/>
           <exclude module="jmxtools"/>
           <exclude module="mail"/>
           <exclude module="jms"/>
       </dependencies>

      val jodaTime = "joda-time" % "joda-time" % "1.6"
      val httpclient = "commons-httpclient" % "commons-httpclient" % "3.1"
    }
  }

  trait TestDependencies {
    val easymock = "org.easymock" % "easymock" % "3.0" % "test"
    val junit = "junit" % "junit" % "4.1" % "test"
    val scalaTest = "org.scalatest" % "scalatest" % "1.2" % "test"
  }

  trait CoreDependencies {
    val log4j = "log4j" % "log4j" % "1.2.15"
    val jopt = "net.sf.jopt-simple" % "jopt-simple" % "3.2"
  }

}
