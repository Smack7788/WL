package core;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

public class DataConverter {

	
	private static final String FILENAME_JSON_FORMATED = "C://Users/Raphael/git/JsonDataConverter/newDataFormated.json";
	private static final int MAX_RBL = 8500;
	private static final String JSON_FILE = "C://Users/Raphael/Desktop/newDataFormated.json";
	private String REQUEST_URL_All = "http://www.wienerlinien.at/ogd_realtime/monitor?%s&sender=Aq5inVKiQsJwRm9c";
	
	private static Logger logger = Logger.getLogger(DataConverter.class); 
	
	public static void main(String[] args) throws IOException {
		
		
		BufferedReader input = null;
		try {
			input = new BufferedReader( new FileReader( JSON_FILE ));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		String inputData;
		int fileCounter = 0;
		int lineCounter = 0;
		Writer output = new BufferedWriter(new FileWriter("C://Users/Raphael/Desktop/splitData/splitData_"+fileCounter+".json"));
		while (( inputData = input.readLine() ) != null) {
			lineCounter++;
			if (lineCounter % 800001 == 0) {
				output.close();
				fileCounter++;
				output = new BufferedWriter(new FileWriter("C://Users/Raphael/Desktop/splitData/splitData_"+fileCounter+".json"));
				output.append(inputData+ "\n");
			}else {
				output.append(inputData+ "\n");
			}
		}
		
		
		
		
		
		
		
		
//		PropertyConfigurator.configureAndWatch("DataConverter.log4j.properties");
//		logger.info("Start job");
		
//		for(int j=0;j<60;j++) {
//			Writer jsonOutput = new BufferedWriter(new FileWriter(JSON_FILE));  //clears file every time
//			DataConverter get_data = new DataConverter();
//			List<String> JSONmonitorlist = get_data.runAll(0, MAX_RBL);
//
//			for (int i = 0; i < JSONmonitorlist.size(); i++) {
//				jsonOutput.append(JSONmonitorlist.get(i) + "\n");
//			}
//			jsonOutput.close();

		
		
//			int counterSuccess = 0;
//			int counterFailed = 0;
//			BufferedReader input = null;
//			try {
//				input = new BufferedReader( new FileReader( JSON_FILE ));
//			} catch (FileNotFoundException e) {
//				e.printStackTrace();
//			}
//
//			Writer output = new BufferedWriter(new FileWriter(FILENAME_JSON_FORMATED));  //clears file every time
//			Date timeR = null;
//			Date timeP = null;
//			String inputData;
//			while (( inputData = input.readLine() ) != null ) {
//				try {
//					JSONObject oldJsonObject = new JSONObject( inputData );
//					logger.info(inputData);
//
//					//				JSONObject _id =oldJsonObject.getJSONObject("_id");
//					//				String id = (String) _id.get("$oid");
//
//					JSONArray lines = (JSONArray) oldJsonObject.get("lines");
//					JSONObject linesObject = lines.getJSONObject(0);
//					String name = (String) linesObject.get("name");
//					String richtungsId = (String) linesObject.get("richtungsId");
//
//					JSONObject departures = linesObject.getJSONObject("departures");
//					JSONArray departure = (JSONArray) departures.get("departure");
//					JSONObject departureObject = departure.getJSONObject(0);
//					JSONObject departureTime = departureObject.getJSONObject("departureTime");
//					String timePlanned = (String) departureTime.get("timePlanned");
//					String timeReal = (String) departureTime.get("timeReal");
//					String serverTime = (String) oldJsonObject.get("serverTime");
//					DateFormat dateFormat = new SimpleDateFormat("yyyy-mm-dd'T'hh:mm:ss.SSS");
//					try {
//						timeP = dateFormat.parse(timePlanned);
//						timeR = dateFormat.parse(timeReal);
//					} catch (ParseException e) {
//						logger.warn(e.getMessage());
//					}
//					int delay = (int) ((timeR.getTime()-timeP.getTime())/1000);
//
//					JSONObject locationStop = oldJsonObject.getJSONObject("locationStop");
//					JSONObject properties = locationStop.getJSONObject("properties");
//					String title = (String) properties.get("title");
//
//					JSONObject geometry = locationStop.getJSONObject("geometry");
//					JSONArray location = (JSONArray) geometry.get("coordinates");
//
//					JSONObject attributes = properties.getJSONObject("attributes");
//					Integer rbl = (Integer) attributes.get("rbl");
//
//					String indexJsonString = "{ \"index\" : { \"_index\" : \"wiener_linien\", \"_type\" : \"departures\" } }";
//					String newJsonString = "{\"name\":\""+ name +"\",\"title\":\""+ title +"\",\"rbl\":"+ rbl +",\"richtungsId\":" + richtungsId +",\"delay\":"+ delay +",\"serverTime\":\""+ serverTime +"\",\"location\":"+ location +"}";
//
//					JSONObject indexJsonObject =new JSONObject(indexJsonString);
//					JSONObject newJsonObject = new JSONObject(newJsonString);	
//
//					output.append(indexJsonObject+ "\n");
//					output.append(newJsonObject+ "\n");
//
//					counterSuccess++;
//				} catch (JSONException e) {
//					logger.warn(e.getMessage());
//					counterFailed++;
//				}
//			}
//			output.close();
//			logger.info(counterSuccess + " have successfully been processed" +"\n"+ counterFailed + " have had an error");

//			logger.info("Start process Script");
//			try {
//				final Process p = Runtime.getRuntime().exec("script.bat");
//				InputStream os = p.getInputStream();
//				InputStreamReader isr = new InputStreamReader(os);
//
//				int data = isr.read();
//				String info ="";
//				int i = 0;
//				while(data != -1){
//					final char theChar = (char) data;
//					info += theChar;
//					data = isr.read();
//					if (i % 1000 == 0) {
//						logger.info(info);
//						info = "";	
//					}
//					i++;
//				}
//				logger.info(info);
//				isr.close();
//				isr = null;
//				os.close();
//				os = null;
//			} catch (final IOException e) {
//				logger.warn(e.getMessage());
//			}
//			logger.info("End process Script");
//
//			try {
//				TimeUnit.SECONDS.sleep(5);
//			} catch (InterruptedException e) {
//				logger.warn(e.getMessage());
//		    }
		}
//	}


	@SuppressWarnings("deprecation")
	private List<String> runAll(int start, int end) {
		List<String> JSONmonitorlist = new ArrayList<String>();
		try {
			List<String> responseJsonMessagelist = loadRealtimeData_all(start, end);
			String messageServerTime = "";
			Integer messageCode = null;
			String messageValue = "";
			for (int i = 0; i < responseJsonMessagelist.size(); ++i) {

				String responseJsonMessage = responseJsonMessagelist.get(i);
				JSONObject responseJsonObject = new JSONObject(responseJsonMessage);
				JSONObject message = responseJsonObject.getJSONObject("message");
				// MetaData of the request
				messageValue = (String) message.get("value");
				messageCode = (Integer) message.get("messageCode");
				messageServerTime = (String) message.get("serverTime");
				logger.info("meta data of the request value=" + messageValue + "; messageCode=" + messageCode
						+ ", messageServerTime=" + messageServerTime);

				JSONObject data = responseJsonObject.getJSONObject("data");
				JSONArray monitorsDetails = (JSONArray) data.get("monitors");

				for (int j = 0; j < monitorsDetails.length(); ++j) {
					JSONObject monitor_single = monitorsDetails.getJSONObject(j);
					monitor_single.put("serverTime", messageServerTime);
					JSONmonitorlist.add(monitor_single.toString());
				}

			}

		} catch (MalformedURLException e) {
			logger.error(e.getMessage());
		} catch (IOException e) {
			logger.error(e.getMessage());
		} catch (JSONException e) {
			logger.error(e.getMessage());
		}
		return JSONmonitorlist;

	}

	private List<String> loadRealtimeData_all(int start, int end)
			throws MalformedURLException, IOException, ProtocolException {
		List<String> finalUrllist = buildURL_all(start, end);
		List<String> JSONresponselist = new ArrayList<String>();

		for (int i = 0; i < finalUrllist.size(); i++) {
			String finalUrl = finalUrllist.get(i);
			URL obj = new URL(finalUrl);
			HttpURLConnection con = (HttpURLConnection) obj.openConnection();
			con.setRequestMethod("GET");
			con.setRequestProperty("User-Agent", "Mozilla/5.0");
			int responseCode = con.getResponseCode();
			logger.info("\nSending 'GET' request to URL : " + finalUrl);
			logger.info("Response Code : " + responseCode);
			BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
			String inputLine;
			StringBuffer response = new StringBuffer();
			while ((inputLine = in.readLine()) != null) {
				response.append(inputLine);
			}
			in.close();

			JSONresponselist.add(response.toString());

		}
		return JSONresponselist;
	}

	private List<String> buildURL_all(int start, int end) {
		List<String> URLarray = new ArrayList<String>();
		logger.info("Requesting rbl numbers " + start + " through " + end);

		for (; start - 1 < end;) {
			String rbltext = String.format("rbl=%d", start);
			for (int i = 1; start < end && i < 500; i++) {
				start++;
				rbltext = rbltext + "&rbl=" + start;
			}
			String finalURL = String.format(REQUEST_URL_All, rbltext);
			URLarray.add(finalURL);
			start++;
		}
		return URLarray;
	}

}
