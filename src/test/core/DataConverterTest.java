package core;


import static org.junit.Assert.*;

import org.apache.log4j.PropertyConfigurator;
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
		String inputRealTime = "{\"locationStop\":{\"geometry\":{\"coordinates\":[16.3915679724817,48.213820788006],\"type\":\"Point\"},\"type\":\"Feature\",\"properties\":{\"municipalityId\":90001,\"name\":\"60200348\",\"municipality\":\"Wien\",\"attributes\":{\"rbl\":310},\"coordName\":\"WGS84\",\"title\":\"Franzensbrücke\",\"type\":\"stop\"}},\"attributes\":{},\"serverTime\":\"2018-01-29T19:00:58.758+0100\",\"lines\":[{\"name\":\"O\",\"richtungsId\":\"1\",\"lineId\":170,\"towards\":\"Praterstern S U\",\"trafficjam\":false,\"barrierFree\":true,\"realtimeSupported\":true,\"type\":\"ptTram\",\"departures\":{\"departure\":[{\"departureTime\":{\"timePlanned\":\"2018-01-29T19:03:00.000+0100\",\"countdown\":0}},{\"departureTime\":{\"timePlanned\":\"2018-01-29T19:10:00.000+0100\",\"timeReal\":\"2018-01-29T19:08:25.000+0100\",\"countdown\":7}},{\"departureTime\":{\"timePlanned\":\"2018-01-29T19:16:00.000+0100\",\"timeReal\":\"2018-01-29T19:16:10.000+0100\",\"countdown\":15}},{\"departureTime\":{\"timePlanned\":\"2018-01-29T19:23:00.000+0100\",\"timeReal\":\"2018-01-29T19:22:43.000+0100\",\"countdown\":21}},{\"departureTime\":{\"timePlanned\":\"2018-01-29T19:31:00.000+0100\",\"timeReal\":\"2018-01-29T19:31:30.000+0100\",\"countdown\":30}},{\"departureTime\":{\"timePlanned\":\"2018-01-29T19:40:00.000+0100\",\"timeReal\":\"2018-01-29T19:40:30.000+0100\",\"countdown\":39}},{\"departureTime\":{\"timePlanned\":\"2018-01-29T19:50:00.000+0100\",\"timeReal\":\"2018-01-29T19:50:30.000+0100\",\"countdown\":49}},{\"departureTime\":{\"timePlanned\":\"2018-01-29T20:00:00.000+0100\",\"countdown\":59},\"vehicle\":{\"name\":\"O\",\"richtungsId\":\"1\",\"attributes\":{},\"towards\":\"Praterstern S U\",\"trafficjam\":false,\"barrierFree\":false,\"realtimeSupported\":true,\"type\":\"ptTram\",\"direction\":\"H\"}},{\"departureTime\":{\"timePlanned\":\"2018-01-29T20:09:00.000+0100\",\"countdown\":68},\"vehicle\":{\"name\":\"O\",\"richtungsId\":\"1\",\"attributes\":{},\"towards\":\"Praterstern S U\",\"trafficjam\":false,\"barrierFree\":false,\"realtimeSupported\":true,\"type\":\"ptTram\",\"direction\":\"H\"}}]},\"direction\":\"H\"}]}";
		
		//,\"timeReal\":\"2018-01-29T19:00:32.000+0100\"
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
		
	}

}
