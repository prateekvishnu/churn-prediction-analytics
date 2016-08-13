package CDR_Genarator;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Scanner;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class JsonGenerator_IPDR {
	@SuppressWarnings("unchecked")
	public static void main(String arg[]) throws IOException {
		java.util.Date date = new java.util.Date();
		System.out.println(new Timestamp(date.getTime()));
		Numbers x = new Numbers();
		Scanner sc = new Scanner(System.in);
		JSONArray array = new JSONArray();
		System.out.println("enter the number of users");
		int users = sc.nextInt();
		long beginTime = Timestamp.valueOf("2013-01-01 00:00:00").getTime();
		long endTime = Timestamp.valueOf("2013-01-31 00:58:00").getTime();
		long diff = endTime - beginTime + 1;
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		for (int i = 0; i < users; i++) {
			long caller_imsi = 250000001200001L + i;
			int number_of_records = 0;
			
			FileWriter file = new FileWriter("C://user//IPDR//" + caller_imsi + ".txt");
			number_of_records = 40 + x.random(40);
			System.out.println("User " + caller_imsi + ", For records " + number_of_records);

			for (int j = 0; j < number_of_records; j++) {
				JSONObject obj = new JSONObject();
				obj.put("imsi_caller", caller_imsi);
				int data_usage = x.random(72000); // bill duration
				obj.put("data_usage", data_usage);
				long timestamp = beginTime + (long) (Math.random() * diff);
				Date randomDate = new Date(timestamp); // timestamp
				obj.put("TimeStamp", dateFormat.format(randomDate));
				array.add(obj);
			}
			file.write(array.toJSONString());
			file.flush();
			array.clear();
			file.close();
		}
		sc.close();
		java.util.Date date2 = new java.util.Date();
		System.out.println(new Timestamp(date2.getTime()));
		System.out.println("Finished");
	}
}
