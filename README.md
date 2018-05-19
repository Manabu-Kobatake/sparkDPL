# sparkDPL
Oracle Direct-Path-Load on Apache SPARK Library

## Description
Load arbitrary DataFrame (DataSet) generated on APACHE SPARK into Oracle Database table at high speed using OCI Direct-Path-Load Engine.

## Demo

## Requirement
1.Apache Spark(2.0.0 over)

2.JDK(7.0 over)

3.Oracle Client(11g,12c)

4.Microsoft Visual Studio

## Usage
1.Edit dbcon.json

    {"user":"SCOTT","password":"TIGER","jdbcUrl":"","sid":"orclpdb"}
    //jdbcUrl is not required

2.Start spark-shell

3.Generate DplCtl Object by executing sparkDPL.scala

    scala> :load C:\tmp\sparkDPL-git\src\main\scala\sparkDPL.scala
    import scala.collection.Iterator
    import org.apache.spark.SparkContext
    import org.apache.spark.sql.SQLContext
    ......
    import Spark2dpl._
    sqlContext: org.apache.spark.sql.SQLContext = org.apache.spark.sql.SQLContext@4c384498
    defined class TargetDB
    defined object DplCtl

4.Prepare the input DataFrame (created from Parquet in this example)

    scala> val df = sqlContext.read.parquet("C:/tmp/PARQUET_TEST")
    
    scala> df.count
    res1: Long = 1000
    
    scala> df.show
    +-------+--------+-------+--------------------+-------+--------------------+-------+
    |NM_COL1| CD_COL2|FG_COL3|             DT_COL4|DV_COL5|             DT_COL6|VC_COL7|
    +-------+--------+-------+--------------------+-------+--------------------+-------+
    |      1|10000001|      0|2017-10-13 00:00:...|     11|2018-01-01 00:00:...|  あいうえお|
    |      2|10000002|      1|2017-10-13 00:00:...|     12|2018-01-01 00:00:...|  かきくけこ|
    |      3|10000003|      0|2017-10-13 00:00:...|     13|2018-01-01 00:00:...|  さしすせそ|
    |      4|10000004|      1|2017-10-13 00:00:...|     14|2018-01-01 00:00:...|  あいうえお|
    |      5|10000005|      0|2017-10-13 00:00:...|     15|2018-01-01 00:00:...|  かきくけこ|
    |      6|10000006|      1|2017-10-13 00:00:...|     16|2018-01-01 00:00:...|  さしすせそ|
    |      7|10000007|      0|2017-10-13 00:00:...|     17|2018-01-01 00:00:...|  あいうえお|
    |      8|10000008|      1|2017-10-13 00:00:...|     18|2018-01-01 00:00:...|  かきくけこ|
    |      9|10000009|      0|2017-10-13 00:00:...|     19|2018-01-01 00:00:...|  さしすせそ|
    |     10|10000010|      1|2017-10-13 00:00:...|     20|2018-01-01 00:00:...|  あいうえお|
    |     11|10000011|      0|2017-10-13 00:00:...|     21|2018-01-01 00:00:...|  かきくけこ|
    |     12|10000012|      1|2017-10-13 00:00:...|     22|2018-01-01 00:00:...|  さしすせそ|
    |     13|10000013|      0|2017-10-13 00:00:...|     23|2018-01-01 00:00:...|  あいうえお|
    |     14|10000014|      1|2017-10-13 00:00:...|     24|2018-01-01 00:00:...|  かきくけこ|
    |     15|10000015|      0|2017-10-13 00:00:...|     25|2018-01-01 00:00:...|  さしすせそ|
    |     16|10000016|      1|2017-10-13 00:00:...|     26|2018-01-01 00:00:...|  あいうえお|
    |     17|10000017|      0|2017-10-13 00:00:...|     27|2018-01-01 00:00:...|  かきくけこ|
    |     18|10000018|      1|2017-10-13 00:00:...|     28|2018-01-01 00:00:...|  さしすせそ|
    |     19|10000019|      0|2017-10-13 00:00:...|     29|2018-01-01 00:00:...|  あいうえお|
    |     20|10000020|      1|2017-10-13 00:00:...|     30|2018-01-01 00:00:...|  かきくけこ|
    +-------+--------+-------+--------------------+-------+--------------------+-------+

6.Execute DplCtl.directPathLoad

    Usage:DplCtl.directPathLoad( src, dest, parallel )
    src:  input DataFrame
    dest: target table name
    parallel: Parallel degree of direct path load

    scala> DplCtl.directPathLoad( df, "PARQUET_TEST", 10 )
    2018/05/19 23:40:27.275 INFO DAGScheduler[dag-scheduler-event-loop]: Got job 2 (foreachPartition at <console>:79) with 4 output partitions
    2018/05/19 23:40:27.275 INFO DAGScheduler[dag-scheduler-event-loop]: Final stage: ResultStage 3 (foreachPartition at <console>:79)
    ...
    2018/05/19 23:40:27.302 INFO SparkContext[dag-scheduler-event-loop]: Created broadcast 5 from broadcast at DAGScheduler.scala:1012
    2018/05/19 23:40:27.305 INFO DAGScheduler[dag-scheduler-event-loop]: Submitting 1 missing tasks from ShuffleMapStage 2 (MapPartitionsRDD[10] at foreachPartition at <console>:79)
    ...
    2018/05/19 23:40:27.330 INFO FileScanRDD[Executor task launch worker-0]: Reading File path: file:///C:/tmp/PARQUET_TEST/part-r-00000-181ceec6-fcb8-4f8e-84c9-333ceb20fc79.snappy.parquet, range: 0-18479, partition values: [empty row]
    ...
    2018/05/19 23:40:27.862 INFO TaskSetManager[dispatcher-event-loop-3]: Starting task 0.0 in stage 3.0 (TID 3, localhost, partition 0, ANY, 5313 bytes)
    2018/05/19 23:40:27.863 INFO TaskSetManager[dispatcher-event-loop-3]: Starting task 1.0 in stage 3.0 (TID 4, localhost, partition 1, ANY, 5313 bytes)
    2018/05/19 23:40:27.865 INFO TaskSetManager[dispatcher-event-loop-3]: Starting task 2.0 in stage 3.0 (TID 5, localhost, partition 2, ANY, 5313 bytes)
    2018/05/19 23:40:27.868 INFO TaskSetManager[dispatcher-event-loop-3]: Starting task 3.0 in stage 3.0 (TID 6, localhost, partition 3, ANY, 5313 bytes)
    2018/05/19 23:40:27.869 INFO Executor[Executor task launch worker-0]: Running task 0.0 in stage 3.0 (TID 3)
    2018/05/19 23:40:27.871 INFO Executor[Executor task launch worker-2]: Running task 2.0 in stage 3.0 (TID 5)
    2018/05/19 23:40:27.871 INFO Executor[Executor task launch worker-3]: Running task 3.0 in stage 3.0 (TID 6)
    2018/05/19 23:40:27.871 INFO Executor[Executor task launch worker-1]: Running task 1.0 in stage 3.0 (TID 4)
    ...
    Spark2dpl load START
    Spark2dpl.dll
    Spark2dpl load END
    2018/05/19 23:40:28.022 INFO Spark2dpl[Executor task launch worker-0]: spark2dpl[java] load start
    2018/05/19 23:40:28.022 INFO Spark2dpl[Executor task launch worker-3]: spark2dpl[java] load start
    2018/05/19 23:40:28.022 INFO Spark2dpl[Executor task launch worker-1]: spark2dpl[java] load start
    2018/05/19 23:40:28.022 INFO Spark2dpl[Executor task launch worker-2]: spark2dpl[java] load start
    2018/05/19 23:40:28.024 INFO Spark2dpl[Executor task launch worker-1]: OCI Initialize start
    2018/05/19 23:40:28.023 INFO Spark2dpl[Executor task launch worker-3]: OCI Initialize start
    2018/05/19 23:40:28.025 INFO Spark2dpl[Executor task launch worker-2]: OCI Initialize start
    2018/05/19 23:40:28.417 INFO Spark2dpl[Executor task launch worker-1]: readyDirectPathLoad start
    2018/05/19 23:40:28.424 INFO Spark2dpl[Executor task launch worker-2]: readyDirectPathLoad start
    2018/05/19 23:40:28.429 INFO Spark2dpl[Executor task launch worker-0]: readyDirectPathLoad start
    2018/05/19 23:40:28.429 INFO Spark2dpl[Executor task launch worker-3]: readyDirectPathLoad start
    2018/05/19 23:40:28.655 INFO Spark2dpl[Executor task launch worker-3]: total record cnt 250 load success
    2018/05/19 23:40:28.656 INFO Spark2dpl[Executor task launch worker-1]: total record cnt 250 load success
    2018/05/19 23:40:28.667 INFO Spark2dpl[Executor task launch worker-2]: total record cnt 250 load success
    2018/05/19 23:40:28.692 INFO Spark2dpl[Executor task launch worker-0]: total record cnt 250 load success
    2018/05/19 23:40:28.719 INFO Spark2dpl[Executor task launch worker-3]: spark2dpl[java] load end. result=true
    result=true
    2018/05/19 23:40:28.721 INFO Spark2dpl[Executor task launch worker-1]: spark2dpl[java] load end. result=true
    result=true
    2018/05/19 23:40:28.723 INFO Spark2dpl[Executor task launch worker-2]: spark2dpl[java] load end. result=true
    result=true
    2018/05/19 23:40:28.728 INFO Executor[Executor task launch worker-1]: Finished task 1.0 in stage 3.0 (TID 4). 1756 bytes result sent to driver
    2018/05/19 23:40:28.731 INFO Executor[Executor task launch worker-2]: Finished task 2.0 in stage 3.0 (TID 5). 1756 bytes result sent to driver
    2018/05/19 23:40:28.732 INFO Executor[Executor task launch worker-3]: Finished task 3.0 in stage 3.0 (TID 6). 1843 bytes result sent to driver
    2018/05/19 23:40:28.736 INFO TaskSetManager[task-result-getter-0]: Finished task 2.0 in stage 3.0 (TID 5) in 872 ms on localhost (1/4)
    2018/05/19 23:40:28.740 INFO TaskSetManager[task-result-getter-3]: Finished task 1.0 in stage 3.0 (TID 4) in 877 ms on localhost (2/4)
    2018/05/19 23:40:28.742 INFO TaskSetManager[task-result-getter-1]: Finished task 3.0 in stage 3.0 (TID 6) in 876 ms on localhost (3/4)
    2018/05/19 23:40:28.911 INFO Spark2dpl[Executor task launch worker-0]: spark2dpl[java] load end. result=true
    result=true
    2018/05/19 23:40:28.916 INFO Executor[Executor task launch worker-0]: Finished task 0.0 in stage 3.0 (TID 3). 1756 bytes result sent to driver
    2018/05/19 23:40:28.919 INFO TaskSetManager[task-result-getter-2]: Finished task 0.0 in stage 3.0 (TID 3) in 1059 ms on localhost (4/4)
    2018/05/19 23:40:28.920 INFO DAGScheduler[dag-scheduler-event-loop]: ResultStage 3 (foreachPartition at <console>:79) finished in 1.060 s
    2018/05/19 23:40:28.922 INFO DAGScheduler[main]: Job 2 finished: foreachPartition at <console>:79, took 1.651431 s
    2018/05/19 23:40:28.922 INFO TaskSchedulerImpl[task-result-getter-2]: Removed TaskSet 3.0, whose tasks have all completed, from pool

    scala>


7.Confirm result loaded into table

    SQL> desc PARQUET_TEST
     名前                                      NULL?    型
     ----------------------------------------- -------- ----------------------------
     NM_COL1                                   NOT NULL NUMBER(10)
     CD_COL2                                            VARCHAR2(8)
     FG_COL3                                   NOT NULL CHAR(1)
     DT_COL4                                            DATE
     DV_COL5                                            CHAR(2)
     DT_COL6                                            TIMESTAMP(6)
     VC_COL7                                            VARCHAR2(100)

    SQL> select count(*) from PARQUET_TEST
      2  ;

      COUNT(*)
    ----------
         1000


## Install
1.Create a new DLL project in MicroSoft Visual Studio

2.Add the following directories to the include directory

    %JAVA_HOME%\include
    %JAVA_HOME%\include\win32\bridge
    %JAVA_HOME%\include\win32
    %ORACLE_HOME%\oci\include

3.Add the following directories to the library directory

    %ORACLE_HOME%\rdbms\lib
    %ORACLE_HOME%\oci\lib
    %ORACLE_HOME%\oci\lib\msvc
    %ORACLE_HOME%\oci\lib\msvc\vc14

4.Build a project and generate Spark2dpl.dll

5.Compile Spark2dpl.java on javac(Specify jar of spark in CLASSPATH)

    %SPARK_HOME%\jars\spark-sql_x.xx.jar
    %SPARK_HOME%\jars\scala-library-x.xx.jar
    %SPARK_HOME%\jars\spark-catalyst_x.xx.jar

6.Customize spark-shell.cmd

Specify the directory of Spark2dpl.class in SPARKCLASSPATH

    SET SPARK_CLASSPATH=C:\work\Spark2Dpl\

Specify directory of Spark2dpl.dll in PATH

    SET PATH=%PATH%;C:\work\spark2dpl\x64\Release\
    
Specify NLS_LANG

    SET NLS_LANG=japanese_japan.JA16SJIS

## Contribution

## Licence
This software is released under the MIT License, see LICENSE.txt.

## Author
[Manabu-Kobatake](https://github.com/Manabu-Kobatake)
