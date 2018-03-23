package core;


import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.List;

import org.apache.log4j.PropertyConfigurator;
import org.json.JSONException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import core.DataConverter;

public class DataConverterTest {

	DataConverter dc = new DataConverter(); 
	
	String headerRealTime = "{\"index\":{\"_index\":\"wiener_linien\",\"_type\":\"departures\"}}";
	String dataStringRealTime = "{\"name\":\"O\",\"stationNumber\":\"60200348\",\"title\":\"Franzensbrücke\",\"lineId\":\"170\",\"rbl\":\"310\",\"richtungsId\":\"1\",\"type\":\"ptTram\",\"barrierFree\":true,\"delay\":-148,\"timePlanned\":\"2018-01-29T19:03:00.000+0100\",\"timeReal\":\"2018-01-29T19:00:32.000+0100\",\"trafficjam\":false,\"serverTime\":\"2018-01-29T19:00:58.758+0100\",\"location\":[16.3915679724817,48.213820788006]}";
	String headerMongoDB = "{\"index\":{\"_index\":\"wiener_linien\",\"_type\":\"departures\",\"_id\":\"5a23e4c2bf3bbf9e80028527\"}}";
	String dataStringMongoDB = "{\"name\":\"D\",\"stationNumber\":\"60201169\",\"title\":\"Schlickgasse\",\"lineId\":\"121\",\"rbl\":\"116\",\"richtungsId\":\"1\",\"type\":\"ptTram\",\"barrierFree\":false,\"delay\":60,\"timePlanned\":\"2017-12-03T12:50:00.000+0100\",\"timeReal\":\"2017-12-03T12:51:00.000+0100\",\"trafficjam\":false,\"serverTime\":\"2017-12-03T12:49:09.992+0100\",\"location\":[16.3642951204559,48.2187649419245]}";
	long sampleUnixTime = 1512082800;
	String sampleWeatherData ="{\"latitude\":48.2082,\"longitude\":16.3738,\"timezone\":\"Europe/Vienna\",\"currently\":{\"time\":1512082800,\"summary\":\"Mostly Cloudy\",\"icon\":\"partly-cloudy-night\",\"precipIntensity\":0,\"precipProbability\":0,\"temperature\":2.04,\"apparentTemperature\":-0.09,\"dewPoint\":-1.56,\"humidity\":0.77,\"pressure\":1009.5,\"windSpeed\":2.01,\"windBearing\":267,\"cloudCover\":0.75,\"visibility\":9.98},\"hourly\":{\"summary\":\"Partly cloudy until night.\",\"icon\":\"partly-cloudy-day\",\"data\":[{\"time\":1512082800,\"summary\":\"Mostly Cloudy\",\"icon\":\"partly-cloudy-night\",\"precipIntensity\":0,\"precipProbability\":0,\"temperature\":2.04,\"apparentTemperature\":-0.09,\"dewPoint\":-1.56,\"humidity\":0.77,\"pressure\":1009.5,\"windSpeed\":2.01,\"windBearing\":267,\"cloudCover\":0.75,\"visibility\":9.98},{\"time\":1512086400,\"summary\":\"Mostly Cloudy\",\"icon\":\"partly-cloudy-night\",\"precipIntensity\":0,\"precipProbability\":0,\"temperature\":1.64,\"apparentTemperature\":-0.24,\"dewPoint\":-1.69,\"humidity\":0.79,\"pressure\":1009.93,\"windSpeed\":1.75,\"windBearing\":259,\"cloudCover\":0.69,\"visibility\":15.22},{\"time\":1512090000,\"summary\":\"Partly Cloudy\",\"icon\":\"partly-cloudy-night\",\"precipIntensity\":0,\"precipProbability\":0,\"temperature\":1.13,\"apparentTemperature\":-1.03,\"dewPoint\":-1.82,\"humidity\":0.81,\"pressure\":1010.24,\"windSpeed\":1.91,\"windBearing\":281,\"cloudCover\":0.31,\"visibility\":16.09},{\"time\":1512093600,\"summary\":\"Partly Cloudy\",\"icon\":\"partly-cloudy-night\",\"precipIntensity\":0,\"precipProbability\":0,\"temperature\":0.86,\"apparentTemperature\":-1.28,\"dewPoint\":-1.98,\"humidity\":0.81,\"pressure\":1010.56,\"windSpeed\":1.86,\"windBearing\":279,\"cloudCover\":0.31,\"visibility\":16.09},{\"time\":1512097200,\"summary\":\"Partly Cloudy\",\"icon\":\"partly-cloudy-night\",\"precipIntensity\":0,\"precipProbability\":0,\"temperature\":0.21,\"apparentTemperature\":0.21,\"dewPoint\":-2.19,\"humidity\":0.84,\"pressure\":1011.08,\"windSpeed\":1.22,\"windBearing\":275,\"cloudCover\":0.31,\"visibility\":15.42},{\"time\":1512100800,\"summary\":\"Clear\",\"icon\":\"clear-night\",\"precipIntensity\":0,\"precipProbability\":0,\"temperature\":0.16,\"apparentTemperature\":-1.53,\"dewPoint\":-2.33,\"humidity\":0.83,\"pressure\":1011.51,\"windSpeed\":1.47,\"windBearing\":307,\"cloudCover\":0.18,\"visibility\":8.29},{\"time\":1512104400,\"summary\":\"Partly Cloudy\",\"icon\":\"partly-cloudy-night\",\"precipIntensity\":0,\"precipProbability\":0,\"temperature\":-0.41,\"apparentTemperature\":-0.41,\"dewPoint\":-2.44,\"humidity\":0.86,\"pressure\":1012.15,\"windSpeed\":0.96,\"windBearing\":282,\"cloudCover\":0.31,\"visibility\":8.9},{\"time\":1512108000,\"summary\":\"Partly Cloudy\",\"icon\":\"partly-cloudy-night\",\"precipIntensity\":0,\"precipProbability\":0,\"temperature\":-0.68,\"apparentTemperature\":-0.68,\"dewPoint\":-2.67,\"humidity\":0.86,\"pressure\":1012.94,\"windSpeed\":0.51,\"windBearing\":326,\"cloudCover\":0.29,\"visibility\":8.42},{\"time\":1512111600,\"summary\":\"Partly Cloudy\",\"icon\":\"partly-cloudy-day\",\"precipIntensity\":0,\"precipProbability\":0,\"temperature\":-0.95,\"apparentTemperature\":-0.95,\"dewPoint\":-2.76,\"humidity\":0.87,\"pressure\":1013.85,\"windSpeed\":0.46,\"windBearing\":14,\"cloudCover\":0.31,\"visibility\":6.68},{\"time\":1512115200,\"summary\":\"Partly Cloudy\",\"icon\":\"partly-cloudy-day\",\"precipIntensity\":0,\"precipProbability\":0,\"temperature\":-0.35,\"apparentTemperature\":-0.35,\"dewPoint\":-2.24,\"humidity\":0.87,\"pressure\":1014.39,\"windSpeed\":0.41,\"windBearing\":112,\"cloudCover\":0.31,\"visibility\":8.14},{\"time\":1512118800,\"summary\":\"Partly Cloudy\",\"icon\":\"partly-cloudy-day\",\"precipIntensity\":0,\"precipProbability\":0,\"temperature\":0.67,\"apparentTemperature\":0.67,\"dewPoint\":-1.72,\"humidity\":0.84,\"pressure\":1014.78,\"windSpeed\":0.49,\"windBearing\":50,\"cloudCover\":0.31,\"visibility\":8.14},{\"time\":1512122400,\"summary\":\"Partly Cloudy\",\"icon\":\"partly-cloudy-day\",\"precipIntensity\":0,\"precipProbability\":0,\"temperature\":1.79,\"apparentTemperature\":1.79,\"dewPoint\":-1.59,\"humidity\":0.78,\"pressure\":1014.87,\"windSpeed\":0.32,\"windBearing\":23,\"cloudCover\":0.31,\"visibility\":8.69},{\"time\":1512126000,\"summary\":\"Partly Cloudy\",\"icon\":\"partly-cloudy-day\",\"precipIntensity\":0,\"precipProbability\":0,\"temperature\":2.89,\"apparentTemperature\":2.89,\"dewPoint\":-1.76,\"humidity\":0.71,\"pressure\":1014.91,\"windSpeed\":0.66,\"windBearing\":119,\"cloudCover\":0.31,\"visibility\":10.51},{\"time\":1512129600,\"summary\":\"Partly Cloudy\",\"icon\":\"partly-cloudy-day\",\"precipIntensity\":0,\"precipProbability\":0,\"temperature\":3.46,\"apparentTemperature\":3.46,\"dewPoint\":-1.59,\"humidity\":0.69,\"pressure\":1014.98,\"windSpeed\":0.91,\"windBearing\":148,\"cloudCover\":0.31,\"visibility\":12.89},{\"time\":1512133200,\"summary\":\"Partly Cloudy\",\"icon\":\"partly-cloudy-day\",\"precipIntensity\":0,\"precipProbability\":0,\"temperature\":3.32,\"apparentTemperature\":2.07,\"dewPoint\":-1.64,\"humidity\":0.7,\"pressure\":1015.28,\"windSpeed\":1.47,\"windBearing\":64,\"cloudCover\":0.31,\"visibility\":12.83},{\"time\":1512136800,\"summary\":\"Partly Cloudy\",\"icon\":\"partly-cloudy-day\",\"precipIntensity\":0,\"precipProbability\":0,\"temperature\":2.63,\"apparentTemperature\":-0.01,\"dewPoint\":-1.46,\"humidity\":0.74,\"pressure\":1015.7,\"windSpeed\":2.61,\"windBearing\":48,\"cloudCover\":0.31,\"visibility\":13.97},{\"time\":1512140400,\"summary\":\"Partly Cloudy\",\"icon\":\"partly-cloudy-night\",\"precipIntensity\":0,\"precipProbability\":0,\"temperature\":1.47,\"apparentTemperature\":-1.51,\"dewPoint\":-1.91,\"humidity\":0.78,\"pressure\":1016.35,\"windSpeed\":2.74,\"windBearing\":34,\"cloudCover\":0.31,\"visibility\":13.62},{\"time\":1512144000,\"summary\":\"Partly Cloudy\",\"icon\":\"partly-cloudy-night\",\"precipIntensity\":0,\"precipProbability\":0,\"temperature\":0.71,\"apparentTemperature\":-0.86,\"dewPoint\":-2.08,\"humidity\":0.82,\"pressure\":1017.3,\"windSpeed\":1.44,\"windBearing\":26,\"cloudCover\":0.31,\"visibility\":10.17},{\"time\":1512147600,\"summary\":\"Partly Cloudy\",\"icon\":\"partly-cloudy-night\",\"precipIntensity\":0,\"precipProbability\":0,\"temperature\":0.33,\"apparentTemperature\":-2.9,\"dewPoint\":-2.84,\"humidity\":0.79,\"pressure\":1018.11,\"windSpeed\":2.77,\"windBearing\":28,\"cloudCover\":0.57,\"visibility\":9.8},{\"time\":1512151200,\"summary\":\"Mostly Cloudy\",\"icon\":\"partly-cloudy-night\",\"precipIntensity\":0,\"precipProbability\":0,\"temperature\":0.02,\"apparentTemperature\":-1.7,\"dewPoint\":-3.09,\"humidity\":0.8,\"pressure\":1018.6,\"windSpeed\":1.48,\"windBearing\":357,\"cloudCover\":0.63,\"visibility\":8.06},{\"time\":1512154800,\"summary\":\"Mostly Cloudy\",\"icon\":\"partly-cloudy-night\",\"precipIntensity\":0,\"precipProbability\":0,\"temperature\":0.27,\"apparentTemperature\":0.27,\"dewPoint\":-3.08,\"humidity\":0.78,\"pressure\":1018.94,\"windSpeed\":0.93,\"windBearing\":3,\"cloudCover\":0.75,\"visibility\":7.29},{\"time\":1512158400,\"summary\":\"Mostly Cloudy\",\"icon\":\"partly-cloudy-night\",\"precipIntensity\":0,\"precipProbability\":0,\"temperature\":0.22,\"apparentTemperature\":-2.84,\"dewPoint\":-3.38,\"humidity\":0.77,\"pressure\":1019.71,\"windSpeed\":2.58,\"windBearing\":27,\"cloudCover\":0.75,\"visibility\":12.13},{\"time\":1512162000,\"summary\":\"Mostly Cloudy\",\"icon\":\"partly-cloudy-night\",\"precipIntensity\":0,\"precipProbability\":0,\"temperature\":-0.53,\"apparentTemperature\":-0.53,\"dewPoint\":-3.39,\"humidity\":0.81,\"pressure\":1020.46,\"windSpeed\":1.33,\"windBearing\":34,\"cloudCover\":0.75,\"visibility\":11.47},{\"time\":1512165600,\"summary\":\"Partly Cloudy\",\"icon\":\"partly-cloudy-night\",\"precipIntensity\":0,\"precipProbability\":0,\"temperature\":-0.92,\"apparentTemperature\":-3.76,\"dewPoint\":-3.47,\"humidity\":0.83,\"pressure\":1020.89,\"windSpeed\":2.19,\"windBearing\":24,\"cloudCover\":0.31,\"visibility\":4.99}]},\"daily\":{\"data\":[{\"time\":1512082800,\"summary\":\"Partly cloudy throughout the day.\",\"icon\":\"partly-cloudy-night\",\"sunriseTime\":1512109550,\"sunsetTime\":1512140665,\"moonPhase\":0.42,\"precipIntensity\":0,\"precipIntensityMax\":0,\"precipProbability\":0,\"temperatureHigh\":3.46,\"temperatureHighTime\":1512129600,\"temperatureLow\":-3.66,\"temperatureLowTime\":1512194400,\"apparentTemperatureHigh\":3.46,\"apparentTemperatureHighTime\":1512129600,\"apparentTemperatureLow\":-6.19,\"apparentTemperatureLowTime\":1512194400,\"dewPoint\":-2.28,\"humidity\":0.8,\"pressure\":1014.88,\"windSpeed\":0.74,\"windBearing\":2,\"cloudCover\":0.42,\"visibility\":10.73,\"temperatureMin\":-0.95,\"temperatureMinTime\":1512111600,\"temperatureMax\":3.46,\"temperatureMaxTime\":1512129600,\"apparentTemperatureMin\":-3.76,\"apparentTemperatureMinTime\":1512165600,\"apparentTemperatureMax\":3.46,\"apparentTemperatureMaxTime\":1512129600}]},\"flags\":{\"sources\":[\"isd\"],\"isd-stations\":[\"110162-99999\",\"110165-99999\",\"110270-99999\",\"110300-99999\",\"110340-99999\",\"110350-99999\",\"110360-99999\",\"110370-99999\",\"110400-99999\",\"110800-99999\",\"110820-99999\",\"110830-99999\",\"110850-99999\",\"110900-99999\",\"111900-99999\",\"113870-99999\"],\"units\":\"si\"},\"offset\":1}";
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		PropertyConfigurator.configureAndWatch("../conf/core.DataConverter.log4j.properties");
	}

	@Before
	public void setUp() throws Exception {
	}
	
	@Test
	public void testConvertDataFromInput() {
		//Test case 1 for real time data
		String inputRealTime = "{\"locationStop\":{\"geometry\":{\"coordinates\":[16.3915679724817,48.213820788006],\"type\":\"Point\"},\"type\":\"Feature\",\"properties\":{\"municipalityId\":90001,\"name\":\"60200348\",\"municipality\":\"Wien\",\"attributes\":{\"rbl\":310},\"coordName\":\"WGS84\",\"title\":\"Franzensbrücke\",\"type\":\"stop\"}},\"attributes\":{},\"serverTime\":\"2018-01-29T19:00:58.758+0100\",\"lines\":[{\"name\":\"O\",\"richtungsId\":\"1\",\"lineId\":170,\"towards\":\"Praterstern S U\",\"trafficjam\":false,\"barrierFree\":true,\"realtimeSupported\":true,\"type\":\"ptTram\",\"departures\":{\"departure\":[{\"departureTime\":{\"timePlanned\":\"2018-01-29T19:03:00.000+0100\",\"timeReal\":\"2018-01-29T19:00:32.000+0100\",\"countdown\":0}},{\"departureTime\":{\"timePlanned\":\"2018-01-29T19:10:00.000+0100\",\"timeReal\":\"2018-01-29T19:08:25.000+0100\",\"countdown\":7}},{\"departureTime\":{\"timePlanned\":\"2018-01-29T19:16:00.000+0100\",\"timeReal\":\"2018-01-29T19:16:10.000+0100\",\"countdown\":15}},{\"departureTime\":{\"timePlanned\":\"2018-01-29T19:23:00.000+0100\",\"timeReal\":\"2018-01-29T19:22:43.000+0100\",\"countdown\":21}},{\"departureTime\":{\"timePlanned\":\"2018-01-29T19:31:00.000+0100\",\"timeReal\":\"2018-01-29T19:31:30.000+0100\",\"countdown\":30}},{\"departureTime\":{\"timePlanned\":\"2018-01-29T19:40:00.000+0100\",\"timeReal\":\"2018-01-29T19:40:30.000+0100\",\"countdown\":39}},{\"departureTime\":{\"timePlanned\":\"2018-01-29T19:50:00.000+0100\",\"timeReal\":\"2018-01-29T19:50:30.000+0100\",\"countdown\":49}},{\"departureTime\":{\"timePlanned\":\"2018-01-29T20:00:00.000+0100\",\"countdown\":59},\"vehicle\":{\"name\":\"O\",\"richtungsId\":\"1\",\"attributes\":{},\"towards\":\"Praterstern S U\",\"trafficjam\":false,\"barrierFree\":false,\"realtimeSupported\":true,\"type\":\"ptTram\",\"direction\":\"H\"}},{\"departureTime\":{\"timePlanned\":\"2018-01-29T20:09:00.000+0100\",\"countdown\":68},\"vehicle\":{\"name\":\"O\",\"richtungsId\":\"1\",\"attributes\":{},\"towards\":\"Praterstern S U\",\"trafficjam\":false,\"barrierFree\":false,\"realtimeSupported\":true,\"type\":\"ptTram\",\"direction\":\"H\"}}]},\"direction\":\"H\"}]}";
		
		String[] actualRealTime = dc.convertDataFromInput(inputRealTime);
		System.out.println(actualRealTime[0]);
		System.out.println(actualRealTime[1]);
		
		String[] expectedRealTime = {headerRealTime,dataStringRealTime};
		
		String actualRealTime0 = actualRealTime[0];
		String expectedRealTime0 = expectedRealTime[0];
		
		assertEquals(expectedRealTime0, actualRealTime0);
		
		String actualRealTime1 = actualRealTime[1];
		String expectedRealTime1 = expectedRealTime[1];
		
		assertEquals(expectedRealTime1, actualRealTime1);
		
		//Test case 2 for MongoDB data
		String inputMongoDB = "{\"_id\":{\"$oid\":\"5a23e4c2bf3bbf9e80028527\"},\"locationStop\":{\"geometry\":{\"coordinates\":[16.3642951204559,48.2187649419245],\"type\":\"Point\"},\"type\":\"Feature\",\"properties\":{\"municipalityId\":90001,\"name\":\"60201169\",\"municipality\":\"Wien\",\"attributes\":{\"rbl\":116},\"coordName\":\"WGS84\",\"title\":\"Schlickgasse\",\"type\":\"stop\"}},\"attributes\":{},\"serverTime\":\"2017-12-03T12:49:09.992+0100\",\"lines\":[{\"name\":\"D\",\"richtungsId\":\"1\",\"towards\":\"Beethovengang\",\"trafficjam\":false,\"barrierFree\":false,\"realtimeSupported\":true,\"type\":\"ptTram\",\"lineId\":121,\"departures\":{\"departure\":[{\"departureTime\":{\"timePlanned\":\"2017-12-03T12:50:00.000+0100\",\"timeReal\":\"2017-12-03T12:51:00.000+0100\",\"countdown\":0}},{\"departureTime\":{\"timePlanned\":\"2017-12-03T12:55:00.000+0100\",\"countdown\":5},\"vehicle\":{\"name\":\"D\",\"richtungsId\":\"1\",\"attributes\":{},\"towards\":\"Marsanogasse\",\"trafficjam\":false,\"barrierFree\":false,\"realtimeSupported\":true,\"type\":\"ptTram\",\"direction\":\"H\"}},{\"departureTime\":{\"timePlanned\":\"2017-12-03T13:00:00.000+0100\",\"countdown\":10}},{\"departureTime\":{\"timePlanned\":\"2017-12-03T13:10:00.000+0100\",\"countdown\":20}},{\"departureTime\":{\"timePlanned\":\"2017-12-03T13:20:00.000+0100\",\"countdown\":30}},{\"departureTime\":{\"timePlanned\":\"2017-12-03T13:30:00.000+0100\",\"countdown\":40}},{\"departureTime\":{\"timePlanned\":\"2017-12-03T13:40:00.000+0100\",\"countdown\":50}},{\"departureTime\":{\"timePlanned\":\"2017-12-03T13:50:00.000+0100\",\"countdown\":60}}]},\"direction\":\"H\"}]}";
				
		String[] actualMongoDB = dc.convertDataFromInput(inputMongoDB);
		System.out.println(actualMongoDB[0]);
		System.out.println(actualMongoDB[1]);
		
		String[] expectedMongoDB = {headerMongoDB,dataStringMongoDB};
		
		String actualMongoDB0 = actualMongoDB[0];
		String expectedMongoDB0 = expectedMongoDB[0];
		
		assertEquals(expectedMongoDB0, actualMongoDB0);
		
		String actualMongoDB1 = actualMongoDB[1];
		String expectedMongoDB1 = expectedMongoDB[1];
		
		assertEquals(expectedMongoDB1, actualMongoDB1);
		
		//Test Case 3 unix to server time converter
		String actualDate = dc.unixToServerTimeConverter(sampleUnixTime);
		System.out.println(actualDate);
		String expectedDate ="2017-12-01T00:00:00.000";
		assertEquals(expectedDate, actualDate);
		
		//Test Case 4 unix to file date converter
		String actualDate2 = dc.unixToFileDateConverter(sampleUnixTime);
		System.out.println(actualDate2);
		String expectedDate2 ="2017_12_01";
		assertEquals(expectedDate2, actualDate2);
		
		//Test Case 5 weather data converter
		//In this test the outputted file is checked
		List<String> input;
		try {
			input = dc.loadDataFile("../sampleData/sampleWeatherData.json");
			dc.weatherDataFormatConverter(input);
		} catch (IOException | JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		
	}

}
