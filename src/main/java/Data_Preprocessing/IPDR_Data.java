package Data_Preprocessing;

import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class IPDR_Data {
	public static void main(String[] args) throws SQLException {
		Connection conn = null;
		PreparedStatement st = null;

		try {

			Class.forName("com.mysql.jdbc.Driver").newInstance();
			String connectionUrl = "jdbc:mysql://localhost:3306/churn_prediction";
			String connectionUser = "root";
			String connectionPassword = "toor";
			conn = DriverManager.getConnection(connectionUrl, connectionUser, connectionPassword);
			System.out.println("connected");
			JSONParser jsonParser = new JSONParser();
			int counter = 0;
			int length = new File("c:\\user\\IPDR").listFiles().length;
			System.out.println(new File("c:\\user\\IPDR").listFiles().length);
			long imsi_user = 250000001200001L;

			for (int i = 0; i < length; i++) {

				JSONArray jsonArray = (JSONArray) jsonParser
						.parse(new FileReader("c:\\user\\IPDR\\" + (imsi_user + i) + ".txt"));
				Iterator<?> iterator = jsonArray.iterator();
				String sql = "INSERT INTO ipdr_data"
						+ "(imsi_caller, data_usage,timestamp) VALUES"
						+ "(?,?,?)";

				st = conn.prepareStatement(sql);

				while (iterator.hasNext()) {

					JSONObject jsonObject = (JSONObject) iterator.next();
					long imsi_caller = imsi_user + i;
					int data_usage = Integer.parseInt(jsonObject.get("data_usage").toString());
					String timestamp = (String) jsonObject.get("TimeStamp");
					
					st.setLong(1, imsi_caller);
					st.setInt(2, data_usage);
					st.setString(3, timestamp);
					
					
					counter += 1;
					st.executeUpdate();
				}
			}
			System.out.println("rows = " + counter);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (st != null) {
				st.close();
			}
			if (conn != null) {
				conn.close();
			}
		}
	}
}
