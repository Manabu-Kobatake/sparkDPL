import scala.collection.Iterator
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.catalyst.ScalaReflection
import java.util.Properties
import java.lang._
import java.util.Properties
import Spark2dpl._

sc.setLogLevel("INFO")
val sqlContext = new SQLContext(sc)

case class TargetDB(
  user : String, password : String, jdbcUrl : String, sid : String
)


//**********************************************************
// DplCtl
//**********************************************************
object DplCtl extends Serializable{
    // Read the Oracle connection information in the JSON file.
    val schema = ScalaReflection.schemaFor[TargetDB].dataType.asInstanceOf[StructType]
    val df = sqlContext.read.schema(schema).json("dbcon.json")
    val target=df.rdd.map{ 
        case Row(user : String, password : String, jdbcUrl : String, sid : String) =>
            TargetDB(user,password,jdbcUrl,sid) 
    }.first()
    
    // Set Oracle connection information in Propety object passed to Java (JNI)
    val props=new Properties()
    props.put("user",target.user)
    props.put("password",target.password)
    //props.put("url",target.jdbcUrl)
    props.put("sid",target.sid)
    println(props)
    
    // Execution of Spark 2 DPL (Java -> JNI -> OCI)
    val rcdProc = (structType : StructType ,props : Properties ,tableName : String ) => (iter : Iterator[Row]) => {
      val dpl = new Spark2dpl()
      props.put("tableName",tableName)
      val result = dpl.execute(iter,structType,props)
      println("result="+result)
    }
    
    // Public method for executing Spark 2 DPL called from spark-shell
    def directPathLoad( df : DataFrame, tableName : String, parallel : Int ) :Unit = {
      val strtp = df.schema
      // The upper limit of parallel degree is preferably the number of CPUs
      if( parallel <= 0 || parallel > 96 )
      {
         println("Invalid specification of degree of parallelism")
      }else{
         df.repartition(parallel).foreachPartition(rcdProc(strtp,props,tableName))
      }
    }
}

//**********************************************************
// Execute DirectPathLoad
//**********************************************************
//val df = sqlContext.read.parquet("C:/tmp/PARQUET_TEST")
//DplCtl.directPathLoad( df, "PARQUET_TEST", 2 )
