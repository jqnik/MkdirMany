# MkdirMany

Provides a small application that drives the HDFS API to create and immediately delete many files at once.
This can be useful to run a stress test on the Namenode and JournalNodes.

Compilation:

```mvn package ```

This will create both, a JAR without dependencies included, as well as a shaded (fat) JAR with all dependencies.


Run like:

```java -cp target/MkdirMany-1.0.0-dependencies.jar:/etc/hadoop/conf mkdirmany.MkdirMany```

The program needs a Hadoop configuation (core-site.xml), which is passed via the classpath here. 
