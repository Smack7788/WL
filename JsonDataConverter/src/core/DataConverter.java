package core;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.Writer;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class DataConverter {

	public static void main(String[] args) throws IOException {
		
//		StringBuffer output = new StringBuffer();
//		Process script; 
//		try {
//			
//			ProcessBuilder builder = new ProcessBuilder("cmd.exe", "/c", "C://Users/Raphael/Desktop/Ex_Files_Elasticsearch_EssT/Exercise Files/data/script.sh");
//		        builder.redirectErrorStream(true);
//		        script = builder.start();
////			script = Runtime.getRuntime().exec("cmd.exe C://Users/Raphael/Desktop/Ex_Files_Elasticsearch_EssT/Exercise Files/data/script.sh");
//			script.waitFor();
//			System.out.println("done");
//			
//			BufferedReader reader =
//                    new BufferedReader(new InputStreamReader(script.getInputStream()));
//
//		String line = "";
//		while ((line = reader.readLine())!= null) {
//			output.append(line + "\n");
//		}
//		} catch (InterruptedException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		
		
		String jsonData = "C://Users/Raphael/Desktop/testDataJson.json";

		int counterSuccess = 0;
		int counterFailed = 0;
		
		BufferedReader input = null;
		try {
			input = new BufferedReader( new FileReader( jsonData ));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Writer output = new BufferedWriter(new FileWriter("C://Users/Raphael/Desktop/master_data/data_deposit/newTestDataJson.json"));  //clears file every time
		Date timeR = null;
		Date timeP = null;
		String inputData;
		while (( inputData = input.readLine() ) != null ) {

		    try {
				JSONObject oldJsonObject = new JSONObject( inputData );
				System.out.println(inputData);
				
				JSONObject _id =oldJsonObject.getJSONObject("_id");
				String id = (String) _id.get("$oid");
				
				JSONArray lines = (JSONArray) oldJsonObject.get("lines");
				JSONObject linesObject = lines.getJSONObject(0);
				String name = (String) linesObject.get("name");
				String richtungsId = (String) linesObject.get("richtungsId");
//				Integer lineId = (Integer) linesObject.get("lineId");
				
				JSONObject departures = linesObject.getJSONObject("departures");
				JSONArray departure = (JSONArray) departures.get("departure");
				JSONObject departureObject = departure.getJSONObject(0);
				JSONObject departureTime = departureObject.getJSONObject("departureTime");
				String timePlanned = (String) departureTime.get("timePlanned");
				String timeReal = (String) departureTime.get("timeReal");
				String serverTime = (String) oldJsonObject.get("serverTime");
				DateFormat dateFormat = new SimpleDateFormat("yyyy-mm-dd'T'hh:mm:ss.SSS");
				try {
					timeP = dateFormat.parse(timePlanned);
					timeR = dateFormat.parse(timeReal);
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				int delay = (int) ((timeR.getTime()-timeP.getTime())/1000);
				
				JSONObject locationStop = oldJsonObject.getJSONObject("locationStop");
				JSONObject properties = locationStop.getJSONObject("properties");
				String title = (String) properties.get("title");
				
				JSONObject geometry = locationStop.getJSONObject("geometry");
				JSONArray location = (JSONArray) geometry.get("coordinates");
				
				JSONObject attributes = properties.getJSONObject("attributes");
				Integer rbl = (Integer) attributes.get("rbl");
			
//				System.out.println(inputData);
//				System.out.println(name +"\n"+ title +"\n"+ rbl +"\n"+ lineId +"\n"+ delay +"\n"+ serverTime+"\n"+ coordinates);
				String indexJsonString = "{ \"index\" : { \"_index\" : \"wiener_linien\", \"_type\" : \"departures\" } }";
				String newJsonString = "{\"name\":\""+ name +"\",\"title\":\""+ title +"\",\"rbl\":"+ rbl +",\"richtungsId\":" + richtungsId +",\"delay\":"+ delay +",\"serverTime\":\""+ serverTime +"\",\"location\":"+ location +"}";
						
				
				JSONObject indexJsonObject =new JSONObject(indexJsonString);
				JSONObject newJsonObject = new JSONObject(newJsonString);
//				System.out.println(indexJsonObject);
//				System.out.println(newJsonObject);
				
				
				
				output.append(indexJsonObject+ "\n");
				output.append(newJsonObject+ "\n");
				
				counterSuccess++;
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				counterFailed++;
			}
		    
		}
		output.close();
		System.out.println(counterSuccess + " have successfully been processed" +"\n"+ counterFailed + " have had an error");

	}
	
}
