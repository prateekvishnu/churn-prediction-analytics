package CDR_Genarator;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Scanner;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class JsonGenerator_CDR {
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
			int mcc_user = 90 + x.random(10);
			int mnc_user = 250 + x.random(70);
			FileWriter file = new FileWriter("C://user//CDR//" + caller_imsi + ".txt");
			number_of_records = 70 + x.random(40);
			System.out.println("User " + caller_imsi + ", For records " + number_of_records);

			for (int j = 0; j < number_of_records; j++) {
				JSONObject obj = new JSONObject();
				obj.put("imsi_caller", caller_imsi);
				long called_imsi = 250000001208001L + x.random(40); // imsi of
				obj.put("imsi_called", called_imsi);
				int duration_seconds = x.random(7200); // bill duration
				obj.put("call_duration", duration_seconds);
				long timestamp = beginTime + (long) (Math.random() * diff);
				Date randomDate = new Date(timestamp); // timestamp
				obj.put("TimeStamp", dateFormat.format(randomDate));
				int lac_user = x.random(65536);
				obj.put("caller_mcc", mcc_user);
				obj.put("caller_mnc", mnc_user); // caller mnc,mcc and lac
				obj.put("caller_lac", lac_user);
				int mcc_called = 90 + x.random(20);
				obj.put("called_mcc", mcc_called);
				int mnc_called = 200 + x.random(100); // called party mnc,mcc
				obj.put("called_mnc", mnc_called);
				int lac_called = x.random(65536);
				obj.put("called_lac", lac_called);
				int international_pack = x.random(2);
				obj.put("international_pack", international_pack);
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
