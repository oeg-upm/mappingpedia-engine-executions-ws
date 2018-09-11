package es.upm.fi.dia.oeg.mappingpedia;

import static org.junit.Assert.*;

import org.json.JSONObject;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.request.HttpRequest;

public class ExecutionsWSTest {
	Logger logger = LoggerFactory.getLogger(this.getClass());
	
	@Test
	public void testExecution1() {
		String url = "http://localhost:8096/executions?dataset_id=cde5a55d-302b-4eff-bb15-acd030605e27&mapping_document_id=0bd0b3ca-bfc4-41bd-b1b6-6b2ada9c6002";
		JSONObject jsonObj = new JSONObject();
		
		try {
			HttpRequest request = Unirest.get(url);
			HttpResponse response = request.asJson();
			logger.info(response.getStatusText());
			assertTrue(response.getStatusText(), true);
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
			
		}
	}
	

}
