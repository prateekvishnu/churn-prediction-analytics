package Data_Preprocessing;

import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class CDR_Data {
	public static void main(String[] args) throws SQLException {
		Connection connection = null;
		PreparedStatement ps = null;

		try {

			Class.forName("com.mysql.jdbc.Driver").newInstance();
			String connectionUrl = "jdbc:mysql://localhost:3306/churn_prediction";
			String connectionUser = "root";
			String connectionPassword = "toor";
			connection = DriverManager.getConnection(connectionUrl, connectionUser, connectionPassword);
			System.out.println("connected");
			JSONParser jsonParser = new JSONParser();
			int counter = 0;
			int length = new File("c:\\user\\CDR").listFiles().length;
			System.out.println(new File("c:\\user\\CDR").listFiles().length);
			long imsi_user = 250000001200001L;
			final int batchSize = 1000;
			int count=0;
			
			for (int i = 0; i < length; i++) {

				JSONArray jsonArray = (JSONArray) jsonParser
						.parse(new FileReader("c:\\user\\CDR\\" + (imsi_user + i) + ".txt"));
				Iterator<?> iterator = jsonArray.iterator();
				String sql = "INSERT INTO cdr_data"
						+ "(imsi_caller, imsi_called, call_duration,timestamp,caller_mcc,caller_mnc,caller_lac,called_mcc,called_mnc,called_lac,international_pack) VALUES"
						+ "(?,?,?,?,?,?,?,?,?,?,?)";

				ps = connection.prepareStatement(sql);
		        connection.setAutoCommit(false);
		        
				
				 
				while (iterator.hasNext()) {

					JSONObject jsonObject = (JSONObject) iterator.next();
					long imsi_caller = imsi_user + i;
					long imsi_called = (Long) jsonObject.get("imsi_called");
					int call_duration = Integer.parseInt(jsonObject.get("call_duration").toString());
					String timestamp = (String) jsonObject.get("TimeStamp");
					
					
					int caller_mcc = Integer.parseInt(jsonObject.get("caller_mcc").toString());
					int caller_mnc = Integer.parseInt(jsonObject.get("caller_mnc").toString());
					int caller_lac = Integer.parseInt(jsonObject.get("caller_lac").toString());
					int called_mcc = Integer.parseInt(jsonObject.get("called_mcc").toString());
					int called_mnc = Integer.parseInt(jsonObject.get("called_mnc").toString());
					int called_lac = Integer.parseInt(jsonObject.get("called_lac").toString());
					int international_pack = Integer.parseInt(jsonObject.get("international_pack").toString());
					ps.setLong(1, imsi_caller);
					ps.setLong(2, imsi_called);
					ps.setInt(3, call_duration);
					ps.setString(4, timestamp);
					ps.setInt(5, caller_mcc); // setting values to sql statement
					ps.setInt(6, caller_mnc);
					ps.setInt(7, caller_lac);
					ps.setInt(8, called_mcc);
					ps.setInt(9, called_mnc);
					ps.setInt(10, called_lac);
					ps.setInt(11, international_pack);
					
				    ps.addBatch();
				    
					counter++;
					
					
					}
				if(counter%batchSize==0){
					ps.executeBatch();
					count++;
					connection.commit();
				}
					
					ps.executeBatch();
					
					connection.commit();
				
					
				

				
				
				
			}

			
			
			
			
			
			System.out.println("rows = " + counter);
			System.out.println(count);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (ps != null) {
				ps.close();
			}
			if (connection != null) {
				connection.close();
			}
		}
	}
}
