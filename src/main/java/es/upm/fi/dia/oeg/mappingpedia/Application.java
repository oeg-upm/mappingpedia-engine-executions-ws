package es.upm.fi.dia.oeg.mappingpedia;

//import org.apache.jena.ontology.OntModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.web.MultipartProperties;



@SpringBootApplication
public class Application {

	public static void main(String[] args) {
		Logger logger = LoggerFactory.getLogger("Application");
		logger.info("Working Directory = " + System.getProperty("user.dir"));
		logger.info("Starting Mappingpedia Engine Application version 1.0.0 ...");
		SpringApplication.run(Application.class, args);

		MultipartProperties multipartProperties = new MultipartProperties();
		multipartProperties.setLocation("./mpe-executions-ws-temp");
		String multiPartPropertiesLocation = multipartProperties.getLocation();
		logger.info("multiPartPropertiesLocation = " + multiPartPropertiesLocation);

		logger.info("Mappingpedia Engine Application WS started.\n\n\n");
	}
}
