import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;


public class Analytics_Train extends Configuration {

	private JavaSparkContext sparkContext;
	private SQLContext sqlContext;

	public Analytics_Train() {
		
		super();
		if (sparkContext == null)
			throw new IllegalArgumentException("sparkContext is not provided.");
		//sparkContext = this.sparkContext;
		//sqlContext = new SQLContext(sparkContext);
		
	}
	
	public PipelineModel train(DataFrame df) {
	


		StringIndexer si = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel");


		RandomForestClassifier rf = new RandomForestClassifier() 				
										.setFeaturesCol("features")
										.setLabelCol("indexedLabel")
										.setImpurity("gini")						//Measures qualtiy of split - gini (default) - Gini Impurity, entropy - information gain
										.setMaxDepth(18) 							//Maximum depth of each tree in the forest. Increase - more expressive and powerful
										.setNumTrees(20)							//Number of trees in the forest. Increase-  decrease the variance in predictions (linear increase in training time), Improves test time accuracy
										.setSeed(5043) 								//reproduces fixed results (A fixed random state)
										.setFeatureSubsetStrategy("auto");			//auto/sqrt, log2, int, float ,None - sets max features


		 Pipeline pipeline = new Pipeline() .setStages(new PipelineStage[] {si,rf}); 
		 PipelineModel model = pipeline.fit(df);
		 return model;
	
	}
}