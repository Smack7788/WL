package core;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class DataConverterTest {

	DataConverter dc = new DataConverter(); 
	
	String header = "{ \"index\" : { \"_index\" : \"wiener_linien\", \"_type\" : \"departures\" } }";
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void testConvertDataFromInput() {
		String input = "";
		
		String[] actual = dc.convertDataFromInput(input);
		
		String[] expected = {header,""};
		
		String actual0 = actual[0];
		String expected0 = expected[0];
		
		assertEquals(expected0, actual0);
		
		String actual1 = actual[1];
		String expected1 = expected[1];
		
		assertEquals(expected1, actual1);
		
		//Test Cases
	}

}
