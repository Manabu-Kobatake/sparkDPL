import java.lang.*;
import java.util.*;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructType;
import scala.collection.Iterator;

public class Spark2dpl implements java.io.Serializable
{
	/** */
	private static String LIB_NAME="Spark2dpl";
	static{
		System.out.println(LIB_NAME  + " load START");
		try{
			System.loadLibrary(LIB_NAME);
			System.out.println(System.mapLibraryName(LIB_NAME));
		}catch(Error e){
			System.err.println("ERROR:");
			e.printStackTrace();
		}
		System.out.println(LIB_NAME  + " load END");
	}
	/**
	 *
	 */
	private native boolean load( Iterator iter, StructType strTyp, Properties props, Logger logger );
	
	private Logger logger = Logger.getLogger(Spark2dpl.class);
	
	/**
	 *
	 */
	public Spark2dpl(){
		super();
	}
	
	/**
	 *
	 */
	public boolean execute( Iterator iter, StructType strTyp, Properties props )
	{
		//Properties props2 = props.clone();
		logger.info("spark2dpl[java] load start");
		boolean result=load(iter,strTyp,props,logger);
		logger.info("spark2dpl[java] load end. result=" + result);
		return result;
	}
}
