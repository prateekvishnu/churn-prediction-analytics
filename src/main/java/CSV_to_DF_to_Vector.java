import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static org.apache.spark.sql.types.DataTypes.createStructField;
import static org.apache.spark.sql.types.DataTypes.createStructType;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;


public class CSV_to_DF_to_Vector extends Configuration {
//	private JavaSparkContext sparkContext;
//	private SQLContext sqlContext;

	public CSV_to_DF_to_Vector() {
		super();
		/*if (sparkContext == null)
			throw new IllegalArgumentException("sparkContext is not provided.");*/
		//sparkContext =sparkContext;
		//sqlContext = new SQLContext(sparkContext);
	}



	public DataFrame covert_csv_to_vector(String location) {
		
		//SparkConf conf = new SparkConf();
	//	conf.setAppName("Reading CSV");
		//conf.setMaster("local[2]");
	//	conf.set("spark.driver.allowMultipleContexts", "true"); 

		

	//	JavaSparkContext sc = new JavaSparkContext(sparkContext);

		SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sparkContext);
	
		
    	StructType schema = createStructType(new StructField[]{
				 	
                 createStructField("Day_Mins", DoubleType, true), 
                 createStructField("Day_Calls", DoubleType, true),
                 createStructField("Avg_Day_Mins", DoubleType,true ), 
                 createStructField("Day_Charge", DoubleType, true), 
                 createStructField("Eve_Mins", DoubleType, true),
                 createStructField("Eve_Calls", DoubleType, true),
                 createStructField("Avg_Evn_Mins", DoubleType, true), 
                 createStructField("Eve_Charge", DoubleType, true), 
                 createStructField("Night_Mins", DoubleType, true), 
                 createStructField("Night_Calls", DoubleType, true),
                 createStructField("Avg_Night_Mins", DoubleType, true), 
                 createStructField("Night_Charge", DoubleType, true), 
                 createStructField("Intl_Mins", DoubleType, true), 
                 createStructField("Intl_Calls", DoubleType, true), 
                 createStructField("Avg_International_Mins", DoubleType, true), 
                 createStructField("Intl_Charge", DoubleType, true), 
                 createStructField("Total_Day_Data", DoubleType, true), 
                 createStructField("Total_Evn_Data", DoubleType, true), 
                 createStructField("Total_Night_Data", DoubleType, true), 
                 createStructField("Total_Data", DoubleType, true), 
                 createStructField("label", DoubleType, true)
    	});
		DataFrame df = sqlContext.read()
				.format("com.databricks.spark.csv")
				.option("inferSchema", "true")
				.option("header", "true")
				.schema(schema)
				.load(location);
		
		VectorAssembler assembler = new VectorAssembler()
 				.setInputCols(new String[]{"Day_Mins", "Day_Calls","Avg_Day_Mins","Day_Charge","Eve_Mins","Eve_Calls","Avg_Evn_Mins","Eve_Charge","Night_Mins","Night_Calls","Avg_Night_Mins","Night_Charge","Intl_Mins","Intl_Calls","Avg_International_Mins","Intl_Charge","Total_Day_Data","Total_Evn_Data","Total_Night_Data","Total_Data"})
 				.setOutputCol("features");

		DataFrame Vector = assembler.transform(df);
    	return Vector;
	}
	

}
