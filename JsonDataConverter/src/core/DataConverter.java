package core;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
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
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;


public class DataConverter {

	private static final String SCRIPT_BAT = "../scripts/script.bat";
	
	private static final int MAX_LINES_FOR_SPLITTED_FILE = 800001;
	private static final int MAGIC_NUMBER_60 = 60;
	private static final int MAGIC_LOOP_NUMBER = 500;
	private static final String NEXT_LINE = "\n";
	private static final String SPLIT_DATA_FILENAME = "../export/splitData_%d.json";
	private static final int MAX_RBL = 8500;
	private static final String FILENAME_JSON_FORMATED = "../export/newDataFormated.json";
	private static final int WAITINGTIME = 5;
	
	private String REQUEST_URL_All = "http://www.wienerlinien.at/ogd_realtime/monitor?%s&sender=Aq5inVKiQsJwRm9c";
	
	public static int MODE_REALTIME = 0;
	public static int MODE_MONGODB  = 1;
	
	
	DateFormat dateFormat = new SimpleDateFormat("yyyy-mm-dd'T'hh:mm:ss.SSS");

	private static Logger logger = Logger.getLogger(DataConverter.class); 

	private List<String> runAll(int start, int end) {
		List<String> JSONmonitorlist = new ArrayList<String>();
		try {
			List<String> responseJsonMessagelist = loadRealtimeData_all(start, end);
			for (String responseJsonMessage :responseJsonMessagelist) {
				JSONObject responseJsonObject = new JSONObject(responseJsonMessage);
				JSONObject message = responseJsonObject.getJSONObject("message");
				// MetaData of the request
				String messageValue = (String) message.get("value");
				Integer messageCode = (Integer) message.get("messageCode");
				String messageServerTime = (String) message.get("serverTime");
				logger.debug("meta data of the request value=" + messageValue + "; messageCode=" + messageCode+ ", messageServerTime=" + messageServerTime);
				JSONObject data = responseJsonObject.getJSONObject("data");
				JSONArray monitorsDetails = (JSONArray) data.get("monitors");
				for (int j = 0; j < monitorsDetails.length(); ++j) {
					JSONObject monitorSingle = monitorsDetails.getJSONObject(j);
					monitorSingle.put("serverTime", messageServerTime);
					JSONmonitorlist.add(monitorSingle.toString());
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

		for (String finalUrl : finalUrllist) {
			URL url = new URL(finalUrl);
			HttpURLConnection con = (HttpURLConnection) url.openConnection();
			con.setRequestMethod("GET");
			con.setRequestProperty("User-Agent", "Mozilla/5.0");
			int responseCode = con.getResponseCode();

			logger.info("Sending 'GET' request to URL: " + finalUrl);
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
			for (int i = 1; start < end && i < MAGIC_LOOP_NUMBER; i++) {
				start++;
				rbltext += "&rbl=" + start;
			}
			String finalURL = String.format(REQUEST_URL_All, rbltext);
			URLarray.add(finalURL);
			start++;
		}
		return URLarray;
	}
	
	private boolean process(int mode) throws IOException {
		logger.info("Start process in mode "+mode);
		//Step 1
		if(mode == MODE_REALTIME)
		
		//Step 2: load json file
		logger.info("Load json file");
		BufferedReader input = loadFormattedDataFile();
		if (input == null)
			return false;

		//Step 3: Split file
		logger.info("Split json file");
		splitFormattedData(input);

		
		String inputData;
		for(int j=0;j<MAGIC_NUMBER_60;j++) {
			
			//Step 4: get real time data
			logger.info("get real time data");
			List<String> jsonMonitorList = runAll(0, MAX_RBL);

			//Step 5: save real time data
			logger.info("save real time data");
			saveRealTimeData(jsonMonitorList);
			
			//Step 6: load json file
			logger.info("Load json file again");
			input = loadFormattedDataFile();

			//Step 7: process json file
			int counterSuccess = 0;
			int counterFailed = 0;
			Writer output = new BufferedWriter(new FileWriter(FILENAME_JSON_FORMATED));  //clears file every time
			while (( inputData = input.readLine() ) != null ) {
				try {
					JSONObject indexJsonObject = null;
					JSONObject newJsonObject   = null;
					String[] jsonArray = convertDataFromInput(inputData);
					if (jsonArray != null){
						indexJsonObject = new JSONObject(jsonArray[0]);
						newJsonObject   = new JSONObject(jsonArray[1]);
						output.append(indexJsonObject+ "\n");
						output.append(newJsonObject+ "\n");
						counterSuccess++;
					} else {
						counterFailed++;
					}
				} catch (JSONException e) {
					logger.warn(e.getMessage());
					counterFailed++;
				}
			}
			output.close();
			logger.info(counterSuccess + " have successfully been processed" +"\n"+ counterFailed + " have had an error");
			
			//Step 8: start script
			logger.info("Start process Script");
			processScript();
			logger.info("End process Script");
			waitAWhile(WAITINGTIME);
		}
		return true;
	}

	private void waitAWhile(int timeInSeconds) {
		try {
			TimeUnit.SECONDS.sleep(timeInSeconds);
		} catch (InterruptedException e) {
			logger.warn(e.getMessage());
		}
	}

	private void processScript() {
		try {
			final Process p = Runtime.getRuntime().exec(SCRIPT_BAT);
			InputStream os = p.getInputStream();
			InputStreamReader isr = new InputStreamReader(os);

			int data = isr.read();
			String info ="";
			int i = 0;
			while(data != -1){
				final char theChar = (char) data;
				info += theChar;
				data = isr.read();
				if (i % 1000 == 0) {
					logger.info(info);
					info = "";	
				}
				i++;
			}
			logger.info(info);
			isr.close();
			isr = null;
			os.close();
			os = null;
		} catch (final IOException e) {
			logger.error(e.getMessage());
		}
	}

	public String[] convertDataFromInput(String inputData) {
		if(inputData == null){
			return null;
		}
		
		logger.debug(inputData);		
		JSONObject oldJsonObject = null;
		try {
			oldJsonObject = new JSONObject(inputData);
		} catch (JSONException e1) {
			logger.error("Problem creating json object");
		}
		
		//				JSONObject _id =oldJsonObject.getJSONObject("_id");
		//				String id = (String) _id.get("$oid");

		JSONObject linesObject = null;
		String name = "";
		String richtungsId = "";
		try {
			JSONArray lines = (JSONArray) oldJsonObject.get("lines");
			linesObject = lines.getJSONObject(0);
			name = (String) linesObject.get("name");
			richtungsId = (String) linesObject.get("richtungsId");
		} catch (JSONException e1) {
			logger.error("Problem reading lines object");
		}
		
	
		String timePlanned = null;
		String timeReal = null;
		JSONObject departureTime = null;
		try {
			JSONObject departures = linesObject.getJSONObject("departures");
			JSONArray departure = (JSONArray) departures.get("departure");
			JSONObject departureObject = departure.getJSONObject(0);
			departureTime = departureObject.getJSONObject("departureTime");
			
		} catch (JSONException e1) {
			logger.error("Problem reading departureTime");
		}

		Date timeP = null;
		try {
			timePlanned = (String) departureTime.get("timePlanned");
			timeP = dateFormat.parse(timePlanned);
		} catch (JSONException | ParseException e1) {
			logger.error("Can not read timePlanned "+e1.getMessage());
		}

		Date timeR = null;
		try {
			timeReal = (String) departureTime.get("timeReal");
			timeR = dateFormat.parse(timeReal);
		} catch (JSONException | ParseException e1) {
			logger.error("Can not read timeReal "+e1.getMessage());
		}
		
		String serverTime = null;
		try {
			serverTime = (String) oldJsonObject.get("serverTime");
		} catch (JSONException e1) {
			logger.error("Can not read serverTime");
		}
		
		Integer delay = calcDelay(timeR,timeP);
		
		String title = "";
		JSONObject locationStop = null;
		JSONObject properties = null;
		try {
			locationStop = oldJsonObject.getJSONObject("locationStop");
			properties = locationStop.getJSONObject("properties");
			title = (String) properties.get("title");
		} catch (JSONException e1) {
			logger.error("Can not read title ");
		}
		JSONArray location = null;
		try {
			JSONObject geometry = locationStop.getJSONObject("geometry");
			location = (JSONArray) geometry.get("coordinates");
		} catch (JSONException e1) {
			logger.error("Can not read coordinates");
		}
		Integer rbl = null;
		try {
			JSONObject attributes = properties.getJSONObject("attributes");
			rbl = (Integer) attributes.get("rbl");
		} catch (JSONException e1) {
			logger.error("Can not read attributes");
		}
		
		StringBuilder sb = new StringBuilder();
		String POST_FIX = "\":\"";
		String POST_FIX2 = "\":";
		String PRE_FIX = "\"";
		String DELIMITER = "\",";
		
		String KEY_VALUE_PAIR = PRE_FIX+"%s"+POST_FIX;
		String KEY_VALUE_PAIR2 = PRE_FIX+"%s"+POST_FIX2;
		
		sb.append("{");
		if(name != null){
			sb.append(String.format(KEY_VALUE_PAIR, "name"));
			sb.append(name);
			sb.append(DELIMITER);
		}
		if(title != null){
			sb.append(String.format(KEY_VALUE_PAIR, "title"));
			sb.append(title);
			sb.append(DELIMITER);
		}
		if(rbl != null){
			sb.append(String.format(KEY_VALUE_PAIR, "rbl"));
			sb.append(rbl);
			sb.append(DELIMITER);
		}
		if(richtungsId != null){
			sb.append(String.format(KEY_VALUE_PAIR, "richtungsId"));
			sb.append(richtungsId);
			sb.append(DELIMITER);
		}
		if(delay != null){
			sb.append(String.format(KEY_VALUE_PAIR2, "delay"));
			sb.append(delay);
			sb.append(",");
		}
		if(serverTime != null){
			sb.append(String.format(KEY_VALUE_PAIR, "serverTime"));
			sb.append(serverTime);
			sb.append(DELIMITER);
		}
		if(location != null){
			sb.append(String.format(KEY_VALUE_PAIR2, "location"));
			sb.append(location);
		}
		//TODO check last character is ,
		sb.append("}");	
		
		/*
		String indexJsonString = "{ \"index\" : { \"_index\" : \"wiener_linien\", \"_type\" : \"departures\" } }";
		String newJsonString = "{\"name\":\""+ name +"\",\"title\":\""+ title +"\",\"rbl\":"+ rbl +",\"richtungsId\":" + richtungsId +",\"delay\":"+ delay
				+",\"serverTime\":\""+ serverTime +"\",\"location\":"+ location +"}";

		 */
		String[] returnValue = new String[2];
		returnValue[0] = "{ \"index\" : { \"_index\" : \"wiener_linien\", \"_type\" : \"departures\" } }";
		returnValue[1] = sb.toString();
		
		return returnValue;
	}

	private Integer calcDelay(Date timeR, Date timeP) {
		Integer delay = null;
		if(timeR != null && timeP != null) {
			delay = (int) ((timeR.getTime()-timeP.getTime())/1000);
		}
		return delay;
	}

	private void saveRealTimeData(List<String> jsonMonitorList) throws IOException {
		Writer jsonOutput = new BufferedWriter(new FileWriter(FILENAME_JSON_FORMATED)); 
		for (int i = 0; i < jsonMonitorList.size(); i++) {
			jsonOutput.append(jsonMonitorList.get(i) + "\n");
		}
		jsonOutput.close();
	}

	private void splitFormattedData(BufferedReader input) throws IOException {
		String inputData;
		int fileCounter = 0;
		int lineCounter = 0;
		String fileName = String.format(SPLIT_DATA_FILENAME, fileCounter);
		Writer output = new BufferedWriter(new FileWriter(fileName));
		while (( inputData = input.readLine() ) != null) {
			lineCounter++;
			if (lineCounter % MAX_LINES_FOR_SPLITTED_FILE == 0) {
				output.close();
				fileCounter++;
				output = new BufferedWriter(new FileWriter(fileName));
				output.append(inputData+ NEXT_LINE);
			}else {
				output.append(inputData+ NEXT_LINE);
			}
		}
	}

	private BufferedReader loadFormattedDataFile() {
		BufferedReader input = null;
		try {
			input = new BufferedReader( new FileReader(FILENAME_JSON_FORMATED));
		} catch (FileNotFoundException e) {
			logger.error(e.getMessage());
		}
		return input;
	}


	public static void main(String[] args) throws IOException {
		PropertyConfigurator.configureAndWatch("../conf/DataConverter.log4j.properties");
		DataConverter converter = new DataConverter();
		boolean ok =converter.process(MODE_REALTIME);
		if(!ok){
			System.exit(1);
		}
		System.exit(0);
	}
}
