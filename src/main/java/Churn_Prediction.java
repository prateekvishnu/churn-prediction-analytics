import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static org.apache.spark.sql.types.DataTypes.createStructField;
import static org.apache.spark.sql.types.DataTypes.createStructType;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;



public class Churn_Prediction {
	public static void main(String[] args) {
		final String TRAINING_DATASET = "datasets\\churn_dataset.csv";
		

		SparkConf conf = new SparkConf();
		conf.setAppName("Reading CSV");
		conf.setMaster("local[2]");
//		conf.set("es-nodes", "localhost:9200");
//		conf.set("es.index.auto.create", "true");
		

		JavaSparkContext sc = new JavaSparkContext(conf);

		SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);
		
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
							.load(TRAINING_DATASET);

		df.printSchema();

		DataFrame[] splits = df.randomSplit(new double[] { 0.8, 0.2 }, 5043);
		DataFrame trainingData = splits[0];
		DataFrame testData = splits[1];


		// Vector Assembler - To convert each column to vector.
		/*
		VectorAssembler assembler = new VectorAssembler()
				.setInputCols(new String[] { "Day_Mins", "Day_Calls", "Day_Charge", "Eve_Mins", "Eve_Calls", "Eve_Charge","Night_Mins", "Night_Calls", "Night_Charge", "Intl_Mins", "Intl_Calls", "Intl_Charge" })
				.setOutputCol("features");
				
				*/
		

		 VectorAssembler assembler = new VectorAssembler()
				 				.setInputCols(new String[]{"Day_Mins", "Day_Calls","Avg_Day_Mins","Day_Charge","Eve_Mins","Eve_Calls","Avg_Evn_Mins","Eve_Charge","Night_Mins","Night_Calls","Avg_Night_Mins","Night_Charge","Intl_Mins","Intl_Calls","Avg_International_Mins","Intl_Charge","Total_Day_Data","Total_Evn_Data","Total_Night_Data","Total_Data"})
				 				.setOutputCol("features");

		DataFrame trainingVector = assembler.transform(trainingData);
		DataFrame testVector = assembler.transform(testData);
		

		StringIndexer si = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel");


		RandomForestClassifier rf = new RandomForestClassifier() 				
										.setFeaturesCol("features")
										.setLabelCol("indexedLabel")
										.setImpurity("gini")				//Measures qualtiy of split - gini (default) - Gini Impurity, entropy - information gain
										.setMaxDepth(18) 					//Maximum depth of each tree in the forest. Increase - more expressive and powerful
										.setNumTrees(20)					//Number of trees in the forest. Increase-  decrease the variance in predictions (linear increase in 
																			//training time), Improves test time accuracy
										.setSeed(5043) 						//reproduces fixed results (A fixed random state)
										.setFeatureSubsetStrategy("auto");	//auto/sqrt, log2, int, float ,None - sets max features


		 Pipeline pipeline = new Pipeline() .setStages(new PipelineStage[] {si,rf}); 
		 PipelineModel model = pipeline.fit(trainingVector);
		
		DataFrame predictions = model.transform(testVector);

		MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
															.setLabelCol("label")
															.setPredictionCol("prediction");

		double accuracy = evaluator.evaluate(predictions);
		//predictions.printSchema();
		System.out.println("Accuracy on test set: "+(accuracy*100));
		
		
	

		//EsSparkSQL.saveToEs(predictions.select("Day_Mins", "Day_Calls","Avg_Day_Mins","Day_Charge","Eve_Mins","Eve_Calls","label", "prediction"),"test\test");
		
		
		/*
		BinaryClassificationMetrics metrics =
			      new BinaryClassificationMetrics(predictions);
		
		 JavaRDD<Tuple2<Object, Object>> roc = metrics.roc().toJavaRDD();
		 System.out.println("ROC curve: " + roc.collect());
		 */
		
	}

}
