import org.apache.spark.ml.PipelineModel;
import org.apache.spark.sql.DataFrame;

public class Churn_Predictor extends Configuration{
	
private static final String TRAINING_DATASET = "datasets\\churn_dataset.csv";
private static final String NEW_DATASET_LOCATION = "datasets\\churn_topredict.csv";




public static void main(String[] args) {
		
		//Configuration x=new Configuration();
		
		//final CSV_to_DF_to_Vector convertor = new CSV_to_DF_to_Vector();
		CSV_to_DF_to_Vector y=new CSV_to_DF_to_Vector();
		
		
		DataFrame df_train=y.covert_csv_to_vector(TRAINING_DATASET);
		
		final Analytics_Train analytics = new Analytics_Train();
		//df_train.show();
		final PipelineModel model = analytics.train(df_train);
		
		
		DataFrame df_test=y.covert_csv_to_vector(NEW_DATASET_LOCATION);
		
		
		DataFrame output = model.transform(df_test);
		
		output.show(500);
	
		
		
		
	}
}