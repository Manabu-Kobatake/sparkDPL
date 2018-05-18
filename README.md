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

    Usage:DplCtl.directPathLoad( src, dest )
    src:  input DataFrame
    dest: target table name

    scala> DplCtl.directPathLoad( df, "PARQUET_TEST" )


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

## Contribution

## Licence
This software is released under the MIT License, see LICENSE.txt.

## Author
[Manabu-Kobatake](https://github.com/Manabu-Kobatake)
