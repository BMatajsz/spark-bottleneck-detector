# Spark bottleneck detector

Spark plugin for automatic bottleneck detection in Spark jobs. Uses `Maven 3.9.9` and `JDK 17`.

To create the JAR run `mvn clean package` inside `/Plugins/JavaPlugins/BottleneckDetector/`.

The following Spark configurations can be used to attach and configure the plugin:

```shell
# To attach (replace <PathToProject> with our project path)
spark.driver.extraJavaOptions="-javaagent:<PathToProject>/Plugins/JavaPlugins/BottleneckDetector/target/spark-bottleneckdetector-1.0.0.jar"
spark.plugins=bottleneckdetector.BottleneckDetectorPlugin
spark.driver.extraClassPath=<PathToProject>/Plugins/JavaPlugins/BottleneckDetector/target/spark-bottleneckdetector-1.0.0.jar
spark.executor.extraClassPath=<PathToProject>/Plugins/JavaPlugins/BottleneckDetector/target/spark-bottleneckdetector-1.0.0.jar

# To configure
spark.pyroscope.server=<ServerAddress>  # for example http://host.docker.internal:4000
spark.pyroscope.profilingInterval=10
spark.pyroscope.samplingDuration=1000
spark.pyroscope.uploadInterval=1000
spark.pyroscope.memory=500k
spark.pyroscope.name=spark-app
spark.detector.cpu.threshold=20
spark.detector.memory.threshold=20
```
