package Data_Preprocessing;

import java.sql.DriverManager;

import java.sql.ResultSet;

import java.sql.SQLException;

import java.sql.Timestamp;

import java.util.ArrayList;

import java.util.HashSet;

import java.util.Set;

import com.mysql.jdbc.Connection;

import com.mysql.jdbc.Statement;

public class UserData {

	// JDBC driver name and database URL
	static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
	static final String DB_URL = "jdbc:mysql://localhost:3306/fraud";

	// Database credentials
	static final String USER = "root";
	static final String PASS = "discover1";
	static Statement stmt = null;

	public static void main(String[] args) throws SQLException, ClassNotFoundException {
		Connection conn = null;
		Class.forName("com.mysql.jdbc.Driver");
		conn = (Connection) DriverManager.getConnection(DB_URL, USER, PASS);
		stmt = (Statement) conn.createStatement();
		ArrayList<Integer> hours = new ArrayList<Integer>();
		ArrayList<String> TimeStamps = new ArrayList<String>();
		ArrayList<Integer> international_pack_check = new ArrayList<Integer>();
		ArrayList<String> caller_party = new ArrayList<String>();
		ArrayList<Long> bill_duration = new ArrayList<Long>();
		Numbers obj = new Numbers();
		Set<String> locationData = new HashSet<String>();
		String caller = null;
		String user_IMSI_numbers = "Select distinct(imsi_caller) from rawdata ";
		ResultSet rs1 = (ResultSet) stmt.executeQuery(user_IMSI_numbers);
		while (rs1.next()) {
			caller = rs1.getString("imsi_caller");
			caller_party.add(caller);
		}
		for (int caller_count = 0; caller_count < caller_party.size(); caller_count++) {
			String sql = "Select * from rawdata where imsi_caller = '" + caller_party.get(caller_count)
					+ "' order by timestamp";
			ResultSet current_user_records = (ResultSet) stmt.executeQuery(sql);
			int current_number_of_records = 0;
			int no_of_weekday_calls = 0;
			int no_of_intl_calls = 0;
			int no_of_weekend_calls = 0;
			int dayCount = 0;
			int nightCount = 0;
			long total_daytime_call_duration = 0;
			long total_nighttime_call_duration = 0;
			long maxbillD = 0;
			long minbillD = 999999999;
			int fraud = 0;
			int weekDay = 0;
			int intlcount = 0;
			long avg_call_duration = 0l;
			int avg_time_diff = 0;
			long billD = 0;
			long total_duration = 0;
			int avg_nighttime_call_duration = 0;
			int mode = 0;
			int international_pack_mode = 0;

			while (current_user_records.next()) {

				// Retrieve by column name
				current_number_of_records += 1;
				caller = current_user_records.getString("imsi_caller");
				billD = current_user_records.getLong("call_duration");
				bill_duration.add(billD);
				total_duration += billD;
				if (maxbillD < billD) {
					maxbillD = billD;
				}
				if (minbillD > billD) {
					minbillD = billD;
				}
				String current_timestamp = current_user_records.getString("timestamp");
				TimeStamps.add(current_timestamp);
				String datetime[] = current_timestamp.split(" ");
				String time[] = datetime[1].split(":");
				int hourCount = Integer.parseInt(time[0]);
				hours.add(Integer.parseInt(time[0]));
				if (hourCount > 6 && hourCount < 20) {
					dayCount += 1;
					total_daytime_call_duration += billD;
				} else {
					nightCount += 1;
					total_nighttime_call_duration += billD;
				}

				boolean weekend = current_user_records.getBoolean("weekend");
				if (weekend == false) {
					weekDay += 1;
				}

				String lacCalled = current_user_records.getString("called_lac");
				String mccCalled = current_user_records.getString("called_mcc");
				String called_party_location = lacCalled + mccCalled; // CONCATINATING
																		// LAC
																		// AND
																		// MCC

				locationData.add(called_party_location);
				boolean intlPack = current_user_records.getBoolean("international_pack");
				if (intlPack == true) {
					international_pack_check.add(1);
				} else {
					international_pack_check.add(0);
				}

				boolean intlCheck = current_user_records.getBoolean("international");
				if (intlCheck == true) {
					intlcount += 1;
				}
			} // end of while loop

			// calculating the average difference in time

			int timedifference = 0;
			for (int i = 1; i < TimeStamps.size(); i++) {
				String timestamp1 = TimeStamps.get(i - 1);
				String timestamp2 = TimeStamps.get(i);
				long time1 = Timestamp.valueOf(timestamp1).getTime();
				long time2 = Timestamp.valueOf(timestamp2).getTime();
				int duration = (int) billD;
				timedifference = ((int) (timedifference + (((time2 - time1) / 1000) - duration)));
				avg_time_diff = timedifference / (current_number_of_records - 1);
			}

			// to calculate number of dialed calls in a month

			int number_of_times_called = current_number_of_records;
			// average call duration
			avg_call_duration = total_duration / number_of_times_called;
			int number_of_daytime_calls = dayCount;
			// no. of night time calls per month
			int number_of_nighttime_calls = nightCount;

			// average duration of day calls
			int avg_daytime_call_duration = 0;
			if (number_of_daytime_calls != 0)
				avg_daytime_call_duration = (int) (total_daytime_call_duration / number_of_daytime_calls);

			// average duration of night calls

			if (number_of_nighttime_calls != 0)
				avg_nighttime_call_duration = (int) (total_nighttime_call_duration / number_of_nighttime_calls);

			// no. of different areas called per month

			int numberOfDifferentLocations = locationData.size();

			// max duration spoke in a month

			long maxDuration = maxbillD;

			// min duration spoke in a month

			long minDuration = minbillD;

			// international pack (1,2,3)

			for (int j = 0; j < international_pack_check.size(); j++) {
				mode = mode + international_pack_check.get(0);
			}

			if (mode > 15) {
				international_pack_mode = 1;
			} else {
				international_pack_mode = 1;
			}

			// no. of weekday calls

			no_of_weekday_calls = weekDay;

			// no. of weekend calls

			no_of_weekend_calls = current_number_of_records - no_of_weekday_calls;

			// no. of international calls

			no_of_intl_calls = intlcount;

			fraud = obj.random(2);

			// to insert data into the user table

			String sqluser = "Insert into user_data values ('" + caller + "','" + number_of_times_called + "','"
					+ avg_call_duration + "','" + avg_time_diff + "','" + number_of_daytime_calls + "','"
					+ avg_daytime_call_duration + "','" + number_of_nighttime_calls + "','"
					+ avg_nighttime_call_duration + "','" + numberOfDifferentLocations + "','" + maxDuration + "','"
					+ minDuration + "','" + no_of_intl_calls + "','" + no_of_weekday_calls + "','" + no_of_weekend_calls
					+ "','" + international_pack_mode + "','" + fraud + "')";

			stmt.executeUpdate(sqluser);

		} // end of final for loop
		System.out.println("Finished");
	}

}
