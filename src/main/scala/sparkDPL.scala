import scala.collection.Iterator
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.SQLContext
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
//**********************************************************
object DplCtl extends Serializable{
    val schema = ScalaReflection.schemaFor[TargetDB].dataType.asInstanceOf[StructType]
    val df = sqlContext.read.schema(schema).json("dbcon.json")
    val target=df.rdd.map{ 
        case Row(user : String, password : String, jdbcUrl : String, sid : String) =>
            TargetDB(user,password,jdbcUrl,sid) 
    }.first()

    val props=new Properties()
    props.put("user",target.user)
    props.put("password",target.password)
    props.put("url",target.jdbcUrl)
    props.put("sid",target.sid)
    println(props)
    
    val rcdProc = (structType : StructType ,props : Properties ,tableName : String ) => (iter : Iterator[Row]) => {
      val dpl = new Spark2dpl()
      props.put("tableName",tableName)
      val result = dpl.execute(iter,structType,props)
      println("result="+result)
    }
    def directPathLoad( df : DataFrame, tableName : String ) :Unit = {
      val strtp = df.schema
      df.repartition(10).foreachPartition(rcdProc(strtp,props,tableName))
      //df.foreachPartition(rcdProc(strtp,props,tableName))
    }
}
val df = sqlContext.read.parquet("C:/tmp/PARQUET_TEST")

//**********************************************************
// Execute DirectPathLoad
//**********************************************************
DplCtl.directPathLoad( df, "PARQUET_TEST" )
