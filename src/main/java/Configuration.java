import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

public class Configuration {
	static final String APP_NAME = "Churn Prediction";
	static final String MASTER_NAME = "local[2]";
	
	
	public JavaSparkContext sparkContext;
	public SQLContext sqlContext;
	
	public Configuration() {
		SparkConf conf = new SparkConf();
		conf.setAppName(APP_NAME);
		conf.setMaster(MASTER_NAME);
		
		sparkContext = new JavaSparkContext(conf);
		sqlContext = new SQLContext(sparkContext);
		
	}
	
	public JavaSparkContext getSparkContext() {
		return sparkContext;
	}
	
	public SQLContext getSQLContext() {
		return sqlContext;
	}

}
