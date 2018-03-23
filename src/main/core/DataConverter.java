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

	private static final String INDEX_HEADER_WEATHER = "{\"index\":{\"_index\":\"wiener_linien_weather\",\"_type\":\"weather\"}}";
	private static final long INITIAL_DATA_COLLECTION_DATE = 1512082800;
	private static final String ES_INDEX = "wiener_linien";
	private static final String ES_INDEX_TYPE = "departures";
	private static final String SCRIPT_BAT = "../scripts/script.bat";
	private static final String SCRIPT_BAT_REAL_TIME = "../scripts/scriptRealTime.bat";
	
	private static final int MAX_LINES_FOR_SPLITTED_FILE = 800001;
	private static final int MAGIC_NUMBER_60 = 60;
	private static final int MAGIC_LOOP_NUMBER = 500;
	private static final String NEXT_LINE = "\n";
	private static final String SPLIT_DATA_FILENAME = "../export/splitData_%d.json";
	private static final int MAX_RBL = 8500;
	private static final String FILENAME_JSON_FORMATED = "../export/newDataFormated.json";
	private static final String REAL_TIME_TEMP_FILE = "../export/realTimeTempFile.json";
	private static final int WAITINGTIME = 5;
	
	private String REQUEST_URL_All = "http://www.wienerlinien.at/ogd_realtime/monitor?%s&sender=Aq5inVKiQsJwRm9c";
	
	public static int MODE_REALTIME = 0;
	public static int MODE_MONGODB  = 1;
	public static int MODE_WEATHER_DATA  = 2;
	
	
	
	DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

	private static Logger logger = Logger.getLogger(DataConverter.class); 

	private List<String> runAll(int start, int end) {
		List<String> JSONmonitorlist = new ArrayList<String>();
		try {
			List<String> responseJsonMessagelist = loadRealtimeDataAll(start, end);
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

	private List<String> loadRealtimeDataAll(int start, int end) throws MalformedURLException, IOException, ProtocolException {
		List<String> finalUrllist = buildURLAll(start, end);
		List<String> JSONresponselist = new ArrayList<String>();

		for (String finalUrl : finalUrllist) {
			URL url = new URL(finalUrl);
			HttpURLConnection con = (HttpURLConnection) url.openConnection();
			con.setRequestMethod("GET");
			con.setRequestProperty("User-Agent", "Mozilla/5.0");
			int responseCode = con.getResponseCode();

			logger.debug("Sending 'GET' request to URL: " + finalUrl);
			logger.debug("Response Code : " + responseCode);

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

	private List<String> buildURLAll(int start, int end) {
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
		if(mode == MODE_REALTIME) {
			for(int j=0;j<MAGIC_NUMBER_60;j++) {
				
				//Step 4: get real time data
				logger.info("get real time data");
				List<String> jsonMonitorList = runAll(0, MAX_RBL);

				//Step 7: process json file
				int counterSuccess = 0;
				int counterFailed = 0;
				Writer output = new BufferedWriter(new FileWriter(REAL_TIME_TEMP_FILE));  //clears file every time
				for (int i = 0; i < jsonMonitorList.size(); i++) {
					try {
						JSONObject indexJsonObject = null;
						JSONObject newJsonObject   = null;
						String[] jsonArray = convertDataFromInput(jsonMonitorList.get(i));
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
				processScriptRealTime();
				logger.info("End process Script");
				waitAWhile(WAITINGTIME);
			}	
		}
		if(mode == MODE_MONGODB) {
			
			List<String> jsonMonitorList = null;
			
			
			//Step 5: save real time data
			logger.info("save real time data");
			saveRealTimeData(jsonMonitorList);
			
			//Step 2: load json file
			logger.info("Load json file");
			List<String> input = loadDataFile(FILENAME_JSON_FORMATED);
			if (input == null)
				return false;
			
			//Step 6: load json file
			logger.info("Load json file again");
			input = loadDataFile(FILENAME_JSON_FORMATED);
			
			

			//Step 3: Split file
			logger.info("Split json file");
			splitFormattedData(input);

		}
		if(mode == MODE_WEATHER_DATA) {
			logger.info("Load weather json file");
			long timeStamp = INITIAL_DATA_COLLECTION_DATE;
			String dateStamp = null;
			
			dateStamp = unixToFileDateConverter(timeStamp);
			List<String> input = loadDataFile("E://dailyWeather_everyHour/dailyWeather_"+dateStamp+".json");
			
			try {
				weatherDataFormatConverter(input);
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
					
			
			
		}
		
		return true;
	}

	public void weatherDataFormatConverter(List<String> input) throws JSONException, IOException {
		JSONObject weatherdata = new JSONObject(input.get(0));
		JSONObject hourly;
		Integer temperature = null;
		Double visibility = null;
		Double precipIntensity = null;
		String summary = "";
		Integer time = null;
		Writer output = new BufferedWriter(new FileWriter("../sampleData/formattedSampleWeatherData.json"));
		try {
			hourly = weatherdata.getJSONObject("hourly");
			JSONArray hourlyData = (JSONArray) hourly.get("data");
			for(int i=0;i<hourlyData.length();i++) {
				JSONObject hourWeather = hourlyData.getJSONObject(i);
				summary = (String) hourWeather.get("summary");
				time = (Integer) hourWeather.get("time");				
				String serverTime = unixToServerTimeConverter(time);
				
				if (hourWeather.get("precipIntensity").getClass().equals(Double.class)){
					precipIntensity = (Double) hourWeather.get("precipIntensity");
				} else {
					precipIntensity = new Double((Integer) hourWeather.get("precipIntensity"));
				}
				if (hourWeather.get("temperature").getClass().equals(Double.class)){
					temperature = ((Double) hourWeather.get("temperature")).intValue();
				} else {
					temperature = (Integer) hourWeather.get("temperature");
				}
				
				
				try {
					if (hourWeather.get("visibility").getClass().equals(Double.class)){
						visibility = (Double) hourWeather.get("visibility");
					} else {
						visibility = new Double((Integer) hourWeather.get("visibility"));
					}
				} catch (JSONException e1) {
					logger.error("Cannot read visibility");
				}
							
				
				StringBuilder dataString = new StringBuilder();
				String POST_FIX = "\":\"";
				String POST_FIX2 = "\":";
				String PRE_FIX = "\"";
				String DELIMITER = "\",";
				
				String KEY_VALUE_PAIR = PRE_FIX+"%s"+POST_FIX;
				String KEY_VALUE_PAIR2 = PRE_FIX+"%s"+POST_FIX2;
				
				dataString.append("{");
				dataString.append(String.format(KEY_VALUE_PAIR, "summary"));
				dataString.append(summary);
				dataString.append(DELIMITER);
				dataString.append(String.format(KEY_VALUE_PAIR, "serverTime"));
				dataString.append(serverTime);
				dataString.append(DELIMITER);		
				dataString.append(String.format(KEY_VALUE_PAIR2, "precipIntensity"));
				dataString.append(precipIntensity);
				dataString.append(",");
				dataString.append(String.format(KEY_VALUE_PAIR2, "temperature"));
				dataString.append(temperature);
				dataString.append(",");
				if(visibility != null){
				dataString.append(String.format(KEY_VALUE_PAIR2, "visibility"));
				dataString.append(visibility);
				}
				boolean lastCharCheck = dataString.toString().endsWith(",");
				if(lastCharCheck) {
					dataString.setLength(dataString.length() - 1);
				}		
				dataString.append("}");
				output.append(INDEX_HEADER_WEATHER.toString()+"\n");
				output.append(dataString.toString()+"\n");
			}
		} catch (JSONException e) {
			logger.error("Cannot read weather data JSON");				
		}
		output.close();
		return;
	}

	public String unixToServerTimeConverter(long time) {
		Date date = new java.util.Date(time*1000L);
		String serverTime = dateFormat.format(date);
		return serverTime;
	}

	public String unixToFileDateConverter(long time) {
		Date date = new java.util.Date(time*1000L); 
		SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy_MM_dd");
		String dateStamp = sdf.format(date);
		return dateStamp;
	}

	private void waitAWhile(int timeInSeconds) {
		try {
			TimeUnit.SECONDS.sleep(timeInSeconds);
		} catch (InterruptedException e) {
			logger.warn(e.getMessage());
		}
	}

	private void processScriptRealTime() {
		try {
			final Process p = Runtime.getRuntime().exec(SCRIPT_BAT_REAL_TIME);
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
					logger.debug(info);
					info = "";	
				}
				i++;
			}
			logger.debug(info);
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
		
		String id = null;
		try {
			JSONObject _id =oldJsonObject.getJSONObject("_id");
			id = (String) _id.get("$oid");
		} catch (JSONException e1) {
			logger.error("Problem reading _id");
		}

		JSONObject linesObject = null;
		String name = "";
		String richtungsId = "";
		try {
			JSONArray linesArray = (JSONArray) oldJsonObject.get("lines");
			linesObject = linesArray.getJSONObject(0);
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
		Integer lineId=null;
		try {
			lineId = (Integer) linesObject.get("lineId");
		} catch (JSONException e1) {
			logger.error("Cannot read lineId");
		}
		String type="";
		try {
			type = (String) linesObject.get("type");
		} catch (JSONException e1) {
			logger.error("Cannot read lines[0].type");
		}
		Boolean barrierFree=null;
		try {
			barrierFree = (Boolean) linesObject.get("barrierFree");
		} catch (JSONException e1) {
			logger.error("Cannot read lines[0].barrierFree");
		}
		Boolean trafficjam = null;
		try {
			trafficjam = (Boolean) linesObject.get("trafficjam");
		} catch (JSONException e1) {
			logger.error("Cannot read lines[0].trafficjam");
		}
		Date timeP = null;
		try {
			timePlanned = (String) departureTime.get("timePlanned");
			timeP = dateFormat.parse(timePlanned);
		} catch (JSONException | ParseException e1) {
			logger.error("Cannot read timePlanned "+e1.getMessage());
		}

		Date timeR = null;
		try {
			timeReal = (String) departureTime.get("timeReal");
			timeR = dateFormat.parse(timeReal);
		} catch (JSONException | ParseException e1) {
			logger.error("Cannot read timeReal "+e1.getMessage());
		}
		
		String serverTime = null;
		try {
			serverTime = (String) oldJsonObject.get("serverTime");
		} catch (JSONException e1) {
			logger.error("Cannot read serverTime");
		}
		
		Integer delay = calcDelay(timeR,timeP);
		
		String title = "";
		String stationNumber ="";
		JSONObject locationStop = null;
		JSONObject properties = null;
		try {
			locationStop = oldJsonObject.getJSONObject("locationStop");
			properties = locationStop.getJSONObject("properties");
			title = (String) properties.get("title");
		} catch (JSONException e1) {
			logger.error("Cannot read title ");
		}
		try {
			stationNumber = (String) properties.get("name");
		} catch (JSONException e1) {
			logger.error("Cannot read stationNumber (properties.name) ");
		}
		JSONArray location = null;
		try {
			JSONObject geometry = locationStop.getJSONObject("geometry");
			location = (JSONArray) geometry.get("coordinates");
		} catch (JSONException e1) {
			logger.error("Cannot read coordinates");
		}
		Integer rbl = null;
		try {
			JSONObject attributes = properties.getJSONObject("attributes");
			rbl = (Integer) attributes.get("rbl");
		} catch (JSONException e1) {
			logger.error("Cannot read attributes");
		}
		
		StringBuilder header = new StringBuilder();
		header.append("{\"index\":{\"_index\":\"");
		header.append(ES_INDEX);
		header.append("\",\"_type\":\"");
		header.append(ES_INDEX_TYPE);
		if(id != null){
			header.append("\",\"_id\":\"");
			header.append(id);
			header.append("\"}}");
		}else
		{header.append("\"}}");}
		
		StringBuilder dataString = new StringBuilder();
		String POST_FIX = "\":\"";
		String POST_FIX2 = "\":";
		String PRE_FIX = "\"";
		String DELIMITER = "\",";
		
		String KEY_VALUE_PAIR = PRE_FIX+"%s"+POST_FIX;
		String KEY_VALUE_PAIR2 = PRE_FIX+"%s"+POST_FIX2;
		
		dataString.append("{");
		if(name != null){
			dataString.append(String.format(KEY_VALUE_PAIR, "name"));
			dataString.append(name);
			dataString.append(DELIMITER);
		}
		if(stationNumber != null){
			dataString.append(String.format(KEY_VALUE_PAIR, "stationNumber"));
			dataString.append(stationNumber);
			dataString.append(DELIMITER);
		}
		if(title != null){
			dataString.append(String.format(KEY_VALUE_PAIR, "title"));
			dataString.append(title);
			dataString.append(DELIMITER);
		}
		if(lineId != null){
			dataString.append(String.format(KEY_VALUE_PAIR, "lineId"));
			dataString.append(lineId);
			dataString.append(DELIMITER);
		}
		if(rbl != null){
			dataString.append(String.format(KEY_VALUE_PAIR, "rbl"));
			dataString.append(rbl);
			dataString.append(DELIMITER);
		}
		if(richtungsId != null){
			dataString.append(String.format(KEY_VALUE_PAIR, "richtungsId"));
			dataString.append(richtungsId);
			dataString.append(DELIMITER);
		}
		if(type != null){
			dataString.append(String.format(KEY_VALUE_PAIR, "type"));
			dataString.append(type);
			dataString.append(DELIMITER);
		}
		if(barrierFree != null){
			dataString.append(String.format(KEY_VALUE_PAIR2, "barrierFree"));
			dataString.append(barrierFree);
			dataString.append(",");
		}
		if(delay != null){
			dataString.append(String.format(KEY_VALUE_PAIR2, "delay"));
			dataString.append(delay);
			dataString.append(",");
		}
		if(timePlanned != null){
			dataString.append(String.format(KEY_VALUE_PAIR, "timePlanned"));
			dataString.append(timePlanned);
			dataString.append(DELIMITER);
		}
		if(timeReal != null){
			dataString.append(String.format(KEY_VALUE_PAIR, "timeReal"));
			dataString.append(timeReal);
			dataString.append(DELIMITER);
		}
		if(trafficjam != null){
			dataString.append(String.format(KEY_VALUE_PAIR2, "trafficjam"));
			dataString.append(trafficjam);
			dataString.append(",");
		}
		if(serverTime != null){
			dataString.append(String.format(KEY_VALUE_PAIR, "serverTime"));
			dataString.append(serverTime);
			dataString.append(DELIMITER);
		}
		if(location != null){
			dataString.append(String.format(KEY_VALUE_PAIR2, "location"));
			dataString.append(location);
		}
//		if(summary != null){
//			dataString.append(",");
//			dataString.append(String.format(KEY_VALUE_PAIR, "summary"));
//			dataString.append(summary);
//			dataString.append(DELIMITER);
//		}
//		if(precipIntensity != null){
//			dataString.append(String.format(KEY_VALUE_PAIR2, "precipIntensity"));
//			dataString.append(precipIntensity);
//			dataString.append(",");
//		}
//		if(temperature != null){
//			dataString.append(String.format(KEY_VALUE_PAIR2, "temperature"));
//			dataString.append(temperature);
//			dataString.append(",");
//		}
//		if(visibility != null){
//			dataString.append(String.format(KEY_VALUE_PAIR2, "visibility"));
//			dataString.append(visibility);
//		}
		boolean lastCharCheck = dataString.toString().endsWith(",");
		if(lastCharCheck) {
			dataString.setLength(dataString.length() - 1);
		}
		dataString.append("}");	
		
		String[] returnValue = new String[2];
		returnValue[0] = header.toString();
		returnValue[1] = dataString.toString();
		
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

	private void splitFormattedData(List<String> input) throws IOException {
		int fileCounter = 0;
		int lineCounter = 0;
		String fileName = String.format(SPLIT_DATA_FILENAME, fileCounter);
		Writer output = new BufferedWriter(new FileWriter(fileName));
		for (String inputData:input) {
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

	public List<String> loadDataFile(String filePath) throws IOException {
		BufferedReader input = null;
		List<String> lines = new ArrayList<String>();
		try {
			input = new BufferedReader( new FileReader(filePath));
			lines.add(input.readLine());
		} catch (FileNotFoundException e) {
			logger.error(e.getMessage());
		}
		return lines;
	}

	public static void main(String[] args) throws IOException {
		PropertyConfigurator.configureAndWatch("../conf/core.DataConverter.log4j.properties");
		DataConverter converter = new DataConverter();
		boolean ok =converter.process(MODE_REALTIME);
		if(!ok){
			System.exit(1);
		}
		System.exit(0);
	}
}
