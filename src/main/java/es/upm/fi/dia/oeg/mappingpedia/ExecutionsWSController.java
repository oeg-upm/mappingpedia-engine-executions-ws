package es.upm.fi.dia.oeg.mappingpedia;

import java.io.File;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import javax.servlet.annotation.MultipartConfig;


import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.request.HttpRequest;
import es.upm.fi.dia.oeg.mappingpedia.controller.MappingExecutionController;
import es.upm.fi.dia.oeg.mappingpedia.model.*;
import es.upm.fi.dia.oeg.mappingpedia.model.result.*;
import es.upm.fi.dia.oeg.mappingpedia.utility.*;
import org.apache.commons.io.FileUtils;
//import org.apache.jena.ontology.OntModel;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RestController
//@RequestMapping(value = "/mappingpedia")
@MultipartConfig(fileSizeThreshold = 20971520)
public class ExecutionsWSController {
    //static Logger logger = LogManager.getLogger("ExecutionsWSController");
    static Logger logger = LoggerFactory.getLogger("ExecutionsWSController");

    private static final String template = "Hello, %s!";
    private final AtomicLong counter = new AtomicLong();

    @RequestMapping(value="/", method= RequestMethod.GET, produces={"application/ld+json"})
    public ExecutionsInbox get() {
        logger.info("GET / ...");
        return new ExecutionsInbox();
    }

    //private OntModel ontModel = MappingPediaEngine.ontologyModel();


//    private GitHubUtility githubClient = MappingPediaEngine.githubClient();
//    private CKANUtility ckanClient = MappingPediaEngine.ckanClient();
//    private JenaClient jenaClient = MappingPediaEngine.jenaClient();
//    private VirtuosoClient virtuosoClient = MappingPediaEngine.virtuosoClient();

    //private MappingExecutionController mappingExecutionController= new MappingExecutionController(ckanClient, githubClient, virtuosoClient, jenaClient);
    private MappingExecutionController mappingExecutionController = MappingExecutionController.apply();
    private MPCJenaUtility jenaClient = mappingExecutionController.jenaClient();
    private MpcCkanUtility ckanClient = mappingExecutionController.ckanClient();

    /*
    @RequestMapping(value="/greeting", method= RequestMethod.GET)
    public GreetingJava getGreeting(@RequestParam(value="name", defaultValue="World") String name) {
        logger.info("/greeting(GET) ...");
        return new GreetingJava(counter.incrementAndGet(), String.format(template, name));
    }
    */

    /*
    @RequestMapping(value="/", method= RequestMethod.GET, produces={"application/ld+json"})
    public Inbox get() {
        logger.info("GET / ...");
        return new Inbox();
    }
    */

    @RequestMapping(value="/", method= RequestMethod.HEAD, produces={"application/ld+json"})
    public ResponseEntity head() {
        logger.info("HEAD / ...");
        HttpHeaders headers = new HttpHeaders();
        headers.add(HttpHeaders.LINK, "<http://mappingpedia-engine.linkeddata.es/inbox>; rel=\"http://www.w3.org/ns/ldp#inbox\"");

        return new ResponseEntity(headers, HttpStatus.CREATED);
    }

    @RequestMapping(value="/inbox", method= RequestMethod.POST)
    public GeneralResult postInbox(
            //@RequestParam(value="notification", required = false) Object notification)
            @RequestBody Object notification
    )
    {
        logger.info("POST /inbox ...");
        logger.info("notification = " + notification);
        return new GeneralResult(HttpStatus.OK.getReasonPhrase(), HttpStatus.OK.value());
    }

    @RequestMapping(value="/inbox", method= RequestMethod.PUT)
    public GeneralResult putInbox(
            //@RequestParam(value="notification", defaultValue="") String notification
            @RequestBody Object notification
    )
    {
        logger.info("PUT /inbox ...");
        logger.info("notification = " + notification);
        return new GeneralResult(HttpStatus.OK.getReasonPhrase(), HttpStatus.OK.value());
    }



    @RequestMapping(value="/mappingexecutions", method= RequestMethod.GET)
    public ListResult getMappingExecution(@RequestParam(value="mapping_document_sha", defaultValue="") String mappingDocumentSHA
            , @RequestParam(value="dataset_distribution_sha", defaultValue="") String datasetDistributionSHA) {

        logger.info("GET /mappingexecutions ...");
        logger.info("mappingDocumentSHA = " + mappingDocumentSHA);
        logger.info("datasetDistributionSHA = " + datasetDistributionSHA);

        return this.mappingExecutionController.findByHash(
                mappingDocumentSHA, datasetDistributionSHA);
    }

    @RequestMapping(value="/executions", method= RequestMethod.GET)
    public ExecuteMappingResult getExecutions(
            @RequestParam(value="dataset_id") String datasetId
            , @RequestParam(value="mapping_document_id") String mappingDocumentId
            , @RequestParam(value="use_cache", defaultValue="true") String useCache
            , @RequestParam(value="callback_url", required = false) String callbackURL
            , @RequestParam(value="query_url", required = false) String queryFileUrl
    )
    {

        logger.info("GET /mappingexecutions ...");
        logger.info("dataset_id = " + datasetId);
        logger.info("mapping_document_id = " + mappingDocumentId);
        logger.info("query_url = " + queryFileUrl);

        try {
            MappingExecution mappingExecution = MappingExecution.apply(datasetId, mappingDocumentId, queryFileUrl);
            return this.mappingExecutionController.executeMapping(mappingExecution);
            /*
            //GET ORGANIZATION ID
            String getDatasetUri = MPCConstants.ENGINE_DATASETS_SERVER() + "dataset?dataset_id=" + datasetId;
            logger.info("Hitting getDatasetUri:" + getDatasetUri);
            HttpResponse<JsonNode> jsonResponse = Unirest.get(getDatasetUri).asJson();
            int responseStatus = jsonResponse.getStatus();
            logger.info("responseStatus = " + responseStatus);
            String organizationId;
            if(responseStatus >= 200 && responseStatus < 300) {
                JSONObject responseResultObject = jsonResponse.getBody().getObject();
                //logger.info("responseResultObject = " + responseResultObject);
                organizationId = responseResultObject.getJSONArray("results").getJSONObject(0).getString("ckan_organization_name");
                logger.info("organizationId = " + organizationId);
            } else {
                ExecuteMappingResult internalError = new ExecuteMappingResult(
                        HttpURLConnection.HTTP_INTERNAL_ERROR, "Unable to obtain organization id"
                );
                return internalError;
            }


            //GET MAPPING DOWNLOAD URL
            String getMappingsUri = MPCConstants.ENGINE_MAPPINGS_SERVER() + "mappings?id=" + mappingDocumentId;
            logger.info("Hitting getMappingsUri:" + getMappingsUri);
            jsonResponse = Unirest.get(getMappingsUri).asJson();
            responseStatus = jsonResponse.getStatus();
            logger.info("responseStatus = " + responseStatus);
            String mdDownloadUrl;
            if(responseStatus >= 200 && responseStatus < 300) {
                JSONObject responseResultObject = jsonResponse.getBody().getObject();
                mdDownloadUrl = responseResultObject.getJSONArray("results").getJSONObject(0).getString("downloadURL");
                logger.info("mdDownloadUrl = " + mdDownloadUrl);
            } else {
                ExecuteMappingResult internalError = new ExecuteMappingResult(
                        HttpURLConnection.HTTP_INTERNAL_ERROR, "Unable to obtain mapping document download URL"
                );
                return internalError;
            }

            //GET DISTRIBUTION DOWNLOAD URL
            String getDistributionsUri = MPCConstants.ENGINE_DATASETS_SERVER() + "distributions?dataset_id=" + datasetId;
            logger.info("Hitting getDistributionsUri:" + getDistributionsUri);
            jsonResponse = Unirest.get(getDistributionsUri).asJson();
            responseStatus = jsonResponse.getStatus();
            logger.info("responseStatus = " + responseStatus);
            String distributionDownloadUrl;
            if(responseStatus >= 200 && responseStatus < 300) {
                JSONObject responseResultObject = jsonResponse.getBody().getObject();
                distributionDownloadUrl = responseResultObject.getJSONArray("results").getJSONObject(0).getString("download_url");
                logger.info("distributionDownloadUrl = " + distributionDownloadUrl);
            } else {
                ExecuteMappingResult internalError = new ExecuteMappingResult(
                        HttpURLConnection.HTTP_INTERNAL_ERROR, "Unable to obtain mapping document download URL"
                );
                return internalError;
            }

            return this.postExecutions(
                    organizationId

                    //Dataset related fields
                    , datasetId
                    , null
                    , null

                    //Distribution related fields
                    , null
                    , null
                    , distributionDownloadUrl
                    , null
                    , null
                    , null

                    //Mapping document related fields
                    , mappingDocumentId
                    , mdDownloadUrl
                    , null
                    , useCache
                    , callbackURL

                    //Execution related field
                    , queryFileUrl
                    , null
                    , null
                    , null
                    , null
                    , null

                    //jdbc related field
                    , null
                    , null
                    , null
                    , null
                    , null
                    , null

            );
            */
        } catch (Exception e) {
            ExecuteMappingResult internalError = new ExecuteMappingResult(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage()
            );
            return internalError;
        }
    }

    /*
    @RequestMapping(value="/greeting/{name}", method= RequestMethod.PUT)
    public GreetingJava putGreeting(@PathVariable("name") String name) {
        logger.info("/greeting(PUT) ...");
        return new GreetingJava(counter.incrementAndGet(),
                String.format(template, name));
    }
    */


    @RequestMapping(value="/ontology/resource_details", method= RequestMethod.GET)
    public OntologyResource getOntologyResourceDetails(
            @RequestParam(value="resource") String resource) {
        logger.info("GET /ontology/resource_details ...");
        String uri = MPEUtility.getClassURI(resource);

        return this.jenaClient.getDetails(uri);
    }

    @RequestMapping(value="/github_repo_url", method= RequestMethod.GET)
    public String getGitHubRepoURL() {
        logger.info("GET /github_repo_url ...");
        return mappingExecutionController.properties().githubRepository();
    }

    @RequestMapping(value="/ckan_datasets", method= RequestMethod.GET)
    public ListResult getCKANDatasets(@RequestParam(value="catalogUrl", required = false) String catalogUrl) {
        if(catalogUrl == null) {
            catalogUrl = mappingExecutionController.properties().ckanURL();
        }
        logger.info("GET /ckanDatasetList ...");
        return MpcCkanUtility.getDatasetList(catalogUrl);
    }

    @RequestMapping(value="/virtuoso_enabled", method= RequestMethod.GET)
    public String getVirtuosoEnabled() {
        logger.info("GET /virtuosoEnabled ...");
        return mappingExecutionController.properties().virtuosoEnabled() + "";
    }

    @RequestMapping(value="/mappingpedia_graph", method= RequestMethod.GET)
    public String getMappingpediaGraph() {
        logger.info("/getMappingPediaGraph(GET) ...");
        return mappingExecutionController.properties().graphName();
    }

    @RequestMapping(value="/ckan_api_action_organization_create", method= RequestMethod.GET)
    public String getCKANAPIActionOrganizationCreate() {
        logger.info("GET /ckanActionOrganizationCreate ...");
        return mappingExecutionController.properties().ckanActionOrganizationCreate();
    }

    @RequestMapping(value="/ckan_api_action_package_create", method= RequestMethod.GET)
    public String getCKANAPIActionPpackageCreate() {
        logger.info("GET /ckanActionPackageCreate ...");
        return mappingExecutionController.properties().ckanActionPackageCreate();
    }

    @RequestMapping(value="/ckan_api_action_resource_create", method= RequestMethod.GET)
    public String getCKANAPIActionResourceCreate() {
        logger.info("GET /getCKANActionResourceCreate ...");
        return mappingExecutionController.properties().ckanActionResourceCreate();
    }

    @RequestMapping(value="/ckan_resource_id", method= RequestMethod.GET)
    public String getCKANResourceIdByResourceUrl(
            @RequestParam(value="package_id", required = true) String packageId
            , @RequestParam(value="resource_url", required = true) String resourceUrl
    ) {
        logger.info("GET /ckan_resource_id ...");
        logger.info("package_id = " + packageId);
        logger.info("resource_url = " + resourceUrl);

        String result = this.ckanClient.getResourceIdByResourceUrl(packageId, resourceUrl);

        return result;

    }

    @RequestMapping(value="/ckan_annotated_resources_ids", method= RequestMethod.GET)
    public ListResult<String> getCKANAnnotatedResourcesIds(
            @RequestParam(value="package_id", required = true) String packageId
    ) {
        logger.info("GET /ckan_annotated_resources_ids ...");
        logger.info("this.ckanClient = " + this.ckanClient);

        ListResult<String> result = this.ckanClient.getAnnotatedResourcesIdsAsListResult(packageId);
        return result;
    }

    @RequestMapping(value="/ckan_resource_url", method= RequestMethod.GET)
    public ListResult<String> getCKANResourceUrl(
            @RequestParam(value="resource_id", required = true) String resourceId
    ) {
        logger.info("GET /ckan_resource_url ...");
        ListResult<String> result = this.ckanClient.getResourcesUrlsAsListResult(resourceId);
        return result;
    }

    @RequestMapping(value="/ckanResource", method= RequestMethod.POST)
    public Integer postCKANResource(
            @RequestParam(value="filePath", required = true) String filePath
            , @RequestParam(value="packageId", required = true) String packageId
    ) {
        logger.info("POST /ckanResource...");
        String ckanURL = mappingExecutionController.properties().ckanURL();
        String ckanKey = mappingExecutionController.properties().ckanKey();

        MpcCkanUtility ckanClient = new MpcCkanUtility(ckanURL, ckanKey);
        File file = new File(filePath);
        try {
            if(!file.exists()) {
                String fileName = file.getName();
                file = new File(fileName);
                FileUtils.copyURLToFile(new URL(filePath), file);
            }
        } catch(Exception e) {
            e.printStackTrace();
        }

        //return ckanUtility.createResource(file.getPath(), packageId);
        return null;
    }

    @RequestMapping(value="/dataset_language/{organizationId}", method= RequestMethod.POST)
    public Integer postDatasetLanguage(
            @PathVariable("organizationId") String organizationId
            , @RequestParam(value="dataset_language", required = true) String datasetLanguage
    ) {
        logger.info("POST /dataset_language ...");
        String ckanURL = mappingExecutionController.properties().ckanURL();
        String ckanKey = mappingExecutionController.properties().ckanKey();

        MpcCkanUtility ckanClient = new MpcCkanUtility(ckanURL, ckanKey);
        return ckanClient.updateDatasetLanguage(organizationId, datasetLanguage);
    }


    @RequestMapping(value="/properties", method= RequestMethod.GET)
    public ListResult getProperties(
            @RequestParam(value="class", required = false, defaultValue="Thing") String aClass
            , @RequestParam(value="direct", required = false, defaultValue="true") String direct
    )
    {
        logger.info("/properties ...");
        logger.info("this.jenaClient = " + this.jenaClient);

        ListResult listResult = this.jenaClient.getProperties(aClass, direct);

        return listResult;
    }





    @RequestMapping(value="/executions", method= RequestMethod.POST)
    public ExecuteMappingResult postExecutions(
            @RequestParam(value="organization_id", required = false) String organizationId

            //Dataset related fields
            , @RequestParam(value="dataset_id", required = false) String pDatasetId
            , @RequestParam(value="ckan_package_id", required = false) String pCkanPackageId
            , @RequestParam(value="ckan_package_name", required = false) String ckanPackageName

            //Distribution related fields
            , @RequestParam(value="ckan_resources_ids", required = false) String ckanResourcesIds
            , @RequestParam(value="distribution_access_url", required = false) String distributionAccessURL
            , @RequestParam(value="distribution_download_url", required = false) String pDistributionDownloadURL
            , @RequestParam(value="distribution_mediatype", required = false, defaultValue="text/csv") String distributionMediaType
            , @RequestParam(value="distribution_encoding", required = false, defaultValue="UTF-8") String distributionEncoding
            , @RequestParam(value="field_separator", required = false) String fieldSeparator

            //Mapping document related fields
            , @RequestParam(value="mapping_document_id", required = false) String pMappingDocumentId
            , @RequestParam(value="mapping_document_download_url", required = false) String pMappingDocumentDownloadURL
            , @RequestParam(value="mapping_language", required = false) String pMappingLanguage
            , @RequestParam(value="use_cache", required = false) String pUseCache
            , @RequestParam(value="callback_url", required = false) String callbackURL

            //Execution related field
            , @RequestParam(value="query_file", required = false) String queryFile
            , @RequestParam(value="query_string", required = false) String queryString
            , @RequestParam(value="output_filename", required = false) String outputFilename
            , @RequestParam(value="output_fileextension", required = false) String outputFileExtension
            , @RequestParam(value="output_mediatype", required = false, defaultValue="text/txt") String outputMediaType
            , @RequestParam(value="update_resource", required = false, defaultValue="true") String pUpdateResource

            //jdbc related field
            , @RequestParam(value="db_username", required = false) String dbUserName
            , @RequestParam(value="db_password", required = false) String dbPassword
            , @RequestParam(value="db_name", required = false) String dbName
            , @RequestParam(value="jdbc_url", required = false) String jdbcURL
            , @RequestParam(value="database_driver", required = false) String databaseDriver
            , @RequestParam(value="database_type", required = false) String databaseType
    )
    {
        logger.info("\n\n\nPOST /executions");
        logger.info("organization_id = " + organizationId);
        logger.info("dataset_id = " + pDatasetId);
        logger.info("ckan_package_id = " + pCkanPackageId);
        logger.info("ckan_package_name = " + ckanPackageName);
        logger.info("distributionDownloadURL = " + pDistributionDownloadURL);
        logger.info("ckan_resources_ids = " + ckanResourcesIds);
        logger.info("mapping_document_id = " + pMappingDocumentId);
        logger.info("mappingDocumentDownloadURL = " + pMappingDocumentDownloadURL);
        logger.info("distribution_encoding = " + distributionEncoding);
        logger.info("use_cache = " + pUseCache);
        logger.info("output_filename = " + outputFilename);
        logger.info("output_fileextension = " + outputFileExtension);
        logger.info("output_mediatype = " + outputMediaType);
        logger.info("callback_url = " + callbackURL);
        logger.info("pUpdateResource = " + pUpdateResource);
        logger.info("query_file = " + queryFile);



        try {
            Agent organization = Agent.apply(organizationId);

//            Dataset dataset = this.datasetController.findOrCreate(
//                    organizationId, pDatasetId, ckanPackageId, ckanPackageName);
//            logger.info("dataset.dctIdentifier() = " + dataset.dctIdentifier());
//            logger.info("dataset.ckanPackageId = " + dataset.ckanPackageId());
            String datasetId = pDatasetId;
            String ckanPackageId = pCkanPackageId;
            if(pDatasetId == null) {
                String datasetsServerUrl = MPCConstants.ENGINE_DATASETS_SERVER() + "datasets";
                if(pCkanPackageId != null && ckanPackageName == null) {
                    ckanPackageId = pCkanPackageId;
                    datasetsServerUrl += "?ckan_package_id=" + pCkanPackageId;
                } else if(ckanPackageName != null && pCkanPackageId  == null) {
                    datasetsServerUrl += "?ckan_package_name=" + ckanPackageName;
                }

                logger.info("Hitting datasets Server Url:" + datasetsServerUrl);
                HttpResponse<JsonNode> jsonResponse = Unirest.get(datasetsServerUrl).asJson();
                int responseStatus = jsonResponse.getStatus();
                logger.info("responseStatus = " + responseStatus);
                if(responseStatus >= 200 && responseStatus < 300) {
                    JSONObject responseResultObject = jsonResponse.getBody().getObject();
                    //logger.info("responseResultObject = " + responseResultObject);
                    datasetId = responseResultObject.getJSONArray("results").getJSONObject(0).getString("id");
                    if(pCkanPackageId == null) {
                        ckanPackageId = responseResultObject.getString("ckan_package_id");
                    }
                }

            }
            logger.info("datasetId = " + datasetId);
            logger.info("ckanPackageId = " + ckanPackageId);


            //List<String> listDistributionDownloadURLs = null;
            //String[] arrayDistributionDownloadURLs = null;
            List<UnannotatedDistribution> unannotatedDistributions = new ArrayList<UnannotatedDistribution>();
            if(ckanResourcesIds != null) {
                List<String> listCKANResourcesIds = Arrays.asList(ckanResourcesIds.split(","));
                logger.info("listCKANResourcesIds = " + listCKANResourcesIds);

                for (String resourceId:listCKANResourcesIds) {
                    UnannotatedDistribution unannotatedDistribution = new UnannotatedDistribution(organizationId, datasetId);
                    unannotatedDistribution.ckanResourceId_$eq(resourceId);
                    String ckanResourceDownloadUrl = this.ckanClient.getResourcesUrlsAsJava(resourceId).iterator().next();
                    unannotatedDistribution.dcatDownloadURL_$eq(ckanResourceDownloadUrl);
                    if(fieldSeparator != null) {
                        unannotatedDistribution.csvFieldSeparator_$eq(fieldSeparator);
                    }
                    unannotatedDistribution.dcatMediaType_$eq(distributionMediaType);

                    //dataset.addDistribution(unannotatedDistribution);
                    unannotatedDistributions.add(unannotatedDistribution);
                }
            } else if(pDistributionDownloadURL != null) {
                List<String> listDistributionDownloadURLs = Arrays.asList(pDistributionDownloadURL.split(","));
                logger.info("listDistributionDownloadURLs = " + listDistributionDownloadURLs);
                for(String distributionDownloadURL:listDistributionDownloadURLs) {
                    UnannotatedDistribution unannotatedDistribution = new UnannotatedDistribution(
                            organizationId, datasetId);
                    String distributionDownloadURLTrimmed = distributionDownloadURL.trim();
                    if(ckanPackageId != null) {
                        String resourceId = this.ckanClient.getResourceIdByResourceUrl(
                                ckanPackageId, distributionDownloadURLTrimmed);
                        unannotatedDistribution.ckanResourceId_$eq(resourceId);
                    }

                    unannotatedDistribution.dcatDownloadURL_$eq(distributionDownloadURLTrimmed);
                    if(fieldSeparator != null) {
                        unannotatedDistribution.csvFieldSeparator_$eq(fieldSeparator);
                    }
                    unannotatedDistribution.dcatMediaType_$eq(distributionMediaType);

                    //dataset.addDistribution(unannotatedDistribution );
                    unannotatedDistributions.add(unannotatedDistribution);
                }
            }

//            MappingDocument md = this.mappingDocumentController.findOrCreate(mappingDocumentId);
//            logger.info("md.dctIdentifier() = " + md.dctIdentifier());

//            if(pMappingDocumentDownloadURL != null) {
//                md.setDownloadURL(pMappingDocumentDownloadURL);
//            }
//            logger.info("md.getDownloadURL() = " + md.getDownloadURL());
//            String mdDownloadURL = md.getDownloadURL();

            String mpeMappingsUrl = MPCConstants.ENGINE_MAPPINGS_SERVER() + "mappings";

            MappingDocument md;
            if(pMappingDocumentId == null) {
                md = new MappingDocument();

                if(pMappingDocumentDownloadURL != null) {
                    md.setDownloadURL(pMappingDocumentDownloadURL);
                    md.hash_$eq(MPEUtility.calculateHash(pMappingDocumentDownloadURL, "UTF-8"));
                }
            } else {
                md = new MappingDocument(pMappingDocumentId);
                if(pMappingDocumentDownloadURL == null) {
                    String getMappingsUrl = mpeMappingsUrl + "?id=" + pMappingDocumentId;
                    logger.info("hitting " + getMappingsUrl);
                    HttpRequest getMappingsRequest = Unirest.get(getMappingsUrl);
                    HttpResponse<JsonNode>  getMappingsResponse = getMappingsRequest.asJson();
                    int getMappingsResponseStatus = getMappingsResponse.getStatus();
                    if(getMappingsResponseStatus >= 200 && getMappingsResponseStatus < 300) {
                        JSONObject getMappingsResponseBodyObject = getMappingsResponse.getBody().getObject();
                        JSONArray getMappingsResponseBodyResultsArray = getMappingsResponseBodyObject.getJSONArray("results");
                        if(getMappingsResponseBodyResultsArray.length() > 0) {
                            JSONObject mappingDocument = getMappingsResponseBodyResultsArray.getJSONObject(0);
                            String mdDownloadURL = mappingDocument.getString("downloadURL");
                            if(mdDownloadURL != null) {
                                md.setDownloadURL(mdDownloadURL);
                            } else if(pMappingDocumentDownloadURL != null) {
                                md.setDownloadURL(pMappingDocumentDownloadURL);
                            }
                        } else {
                            if(pMappingDocumentDownloadURL != null) {
                                md.setDownloadURL(pMappingDocumentDownloadURL);
                                md.hash_$eq(MPEUtility.calculateHash(pMappingDocumentDownloadURL, "UTF-8"));
                            }
                        }
                    }
                } else {
                    md.setDownloadURL(pMappingDocumentDownloadURL);
                }
            }
            String mdId = md.dctIdentifier();
            String mdHash = md.hash();
            String mdDownloadUrl = md.getDownloadURL();
            logger.info("md.getDownloadURL() = " + md.getDownloadURL());


            String mdLanguage;
            if(pMappingLanguage != null) {
                mdLanguage = pMappingLanguage;
            } else {
                mdLanguage = MpcUtility.detectMappingLanguage(mdDownloadUrl);
            }
            logger.info("mdLanguage = " + mdLanguage);

            if(pMappingDocumentId == null) {
                String postMappingsUrl = mpeMappingsUrl + "/" + organizationId + "/" + datasetId;
                //HttpRequestWithBody request = Unirest.post(postMappingsUrl);

                if(pMappingDocumentDownloadURL != null) {
                    logger.info("setting parameters for:" + postMappingsUrl);
                }
                try {
                    HttpResponse postMappingsResponse = Unirest.post(postMappingsUrl)
                            .field("mapping_document_download_url", mdDownloadUrl)
                            .field("mapping_language", md.mappingLanguage()).asJson();

                    logger.info("postMappingsResponse = " + postMappingsResponse);
                } catch(Exception e) {
                    e.printStackTrace();
                }
            }


            JDBCConnection jdbcConnection = null;
            if(dbUserName != null && dbPassword != null && dbName != null
                    && jdbcURL != null && databaseDriver != null && databaseType != null) {
                jdbcConnection = new JDBCConnection(dbUserName, dbPassword
                        , dbName, jdbcURL
                        , databaseDriver, databaseType);
            }


            Boolean useCache = MPEUtility.stringToBoolean(pUseCache);
            Boolean updateResource = MPEUtility.stringToBoolean(pUpdateResource);
            MappingExecution mappingExecution = new MappingExecution(
                    //md
                    unannotatedDistributions
                    , jdbcConnection
                    , queryFile
                    , outputFilename
                    , outputFileExtension
                    , outputMediaType
                    //, true
                    , true
                    , true
                    , useCache
                    , callbackURL
                    , updateResource
                    , mdId
                    , mdHash
                    , mdDownloadUrl
                    , mdLanguage
            );
            mappingExecution.storeToCKAN_$eq(true);
            //IN THIS PARTICULAR CASE WE HAVE TO STORE THE EXECUTION RESULT ON CKAN
            return mappingExecutionController.executeMapping(mappingExecution);

            /*
        MappingExecution mappingExecution = new MappingExecution(md, dataset);
        mappingExecution.setStoreToCKAN("true");
        mappingExecution.outputFileName_$eq(outputFilename);
        mappingExecution.queryFilePath_$eq(queryFile);
        return MappingExecutionController.executeMapping2(mappingExecution);
*/
        } catch (Exception e) {
            e.printStackTrace();
            String errorMessage = "Error occured: " + e.getMessage();
            logger.error("mapping execution failed: " + errorMessage);
            ExecuteMappingResult executeMappingResult = new ExecuteMappingResult(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage()
            );
            return executeMappingResult;
        }
    }

/*    //TODO REFACTOR THIS; MERGE /executions with /executions2
    //@RequestMapping(value="/executions1/{organizationId}/{datasetId}/{mappingFilename:.+}"
//            , method= RequestMethod.POST)
    @RequestMapping(value="/executions1/{organizationId}/{datasetId}/{mappingDocumentId}"
            , method= RequestMethod.POST)
    public ExecuteMappingResult postExecutions1(
            @PathVariable("organization_id") String organizationId

            , @PathVariable("dataset_id") String datasetId
            , @RequestParam(value="distribution_access_url", required = false) String distributionAccessURL
            , @RequestParam(value="distribution_download_url", required = false) String distributionDownloadURL
            , @RequestParam(value="distribution_mediatype", required = false, defaultValue="text/csv") String distributionMediaType
            , @RequestParam(value="field_separator", required = false) String fieldSeparator

            , @RequestParam(value="mapping_document_id", required = false) String mappingDocumentId
            , @RequestParam(value="mapping_document_download_url", required = false) String mappingDocumentDownloadURL
            , @RequestParam(value="mapping_language", required = false) String pMappingLanguage

            , @RequestParam(value="query_file", required = false) String queryFile
            , @RequestParam(value="output_filename", required = false) String outputFilename

            , @RequestParam(value="db_username", required = false) String dbUserName
            , @RequestParam(value="db_password", required = false) String dbPassword
            , @RequestParam(value="db_name", required = false) String dbName
            , @RequestParam(value="jdbc_url", required = false) String jdbc_url
            , @RequestParam(value="database_driver", required = false) String databaseDriver
            , @RequestParam(value="database_type", required = false) String databaseType

            , @RequestParam(value="use_cache", required = false) String pUseCache
            //, @PathVariable("mappingFilename") String mappingFilename
    )
    {
        logger.info("POST /executions1/{organizationId}/{datasetId}/{mappingDocumentId}");
        logger.info("mapping_document_id = " + mappingDocumentId);

        Agent organization = new Agent(organizationId);

        Dataset dataset = new Dataset(organization, datasetId);
        Distribution distribution = new Distribution(dataset);
        if(distributionAccessURL != null) {
            distribution.dcatAccessURL_$eq(distributionAccessURL);
        }
        if(distributionDownloadURL != null) {
            distribution.dcatDownloadURL_$eq(distributionDownloadURL);
        } else {
            distribution.dcatDownloadURL_$eq(this.githubClient.getDownloadURL(distributionAccessURL));
        }
        if(fieldSeparator != null) {
            distribution.cvsFieldSeparator_$eq(fieldSeparator);
        }
        distribution.dcatMediaType_$eq(distributionMediaType);
        dataset.addDistribution(distribution);


        MappingDocument md = new MappingDocument();
        if(mappingDocumentDownloadURL != null) {
            md.setDownloadURL(mappingDocumentDownloadURL);
        } else {
            if(mappingDocumentId != null) {
                MappingDocument foundMappingDocument = this.mappingDocumentController.findMappingDocumentsByMappingDocumentId(mappingDocumentId);
                md.setDownloadURL(foundMappingDocument.getDownloadURL());
            } else {
                //I don't know that to do here, Ahmad will handle
            }
        }

        if(pMappingLanguage != null) {
            md.mappingLanguage_$eq(pMappingLanguage);
        } else {
            String mappingLanguage = MappingDocumentController.detectMappingLanguage(mappingDocumentDownloadURL);
            logger.info("mappingLanguage = " + mappingLanguage);
            md.mappingLanguage_$eq(mappingLanguage);
        }


        JDBCConnection jdbcConnection = new JDBCConnection(dbUserName, dbPassword
                , dbName, jdbc_url
                , databaseDriver, databaseType);


        Boolean useCache = MappingPediaUtility.stringToBoolean(pUseCache);
        try {
            //IN THIS PARTICULAR CASE WE HAVE TO STORE THE EXECUTION RESULT ON CKAN
            return mappingExecutionController.executeMapping(md, dataset, queryFile, outputFilename
                    , true, true, true, jdbcConnection
                    , useCache

            );
        } catch (Exception e) {
            e.printStackTrace();
            String errorMessage = "Error occured: " + e.getMessage();
            logger.error("mapping execution failed: " + errorMessage);
            ExecuteMappingResult executeMappingResult = new ExecuteMappingResult(
                    HttpURLConnection.HTTP_INTERNAL_ERROR, "Internal Error"
                    , null, null
                    , null
                    , null, null
                    , null
                    , null
                    , null, null
            );
            return executeMappingResult;
        }
    }*/








    @RequestMapping(value = "/datasets_mappings_execute", method= RequestMethod.POST)
    public AddDatasetMappingExecuteResult postDatasetsAndMappingsThenExecute(
            @RequestParam("organization_id") String organizationID

            , @RequestParam(value="dataset_title", required = false) String datasetTitle
            , @RequestParam(value="dataset_keywords", required = false) String datasetKeywords
            , @RequestParam(value="dataset_language", required = false, defaultValue="en") String datasetLanguage
            , @RequestParam(value="dataset_description", required = false) String datasetDescription

            , @RequestParam(value="distribution_access_url", required = false) String distributionAccessURL
            , @RequestParam(value="distribution_download_url", required = false) String distributionDownloadURL
            , @RequestParam(value="distribution_file", required = false) MultipartFile distributionMultipartFile
            , @RequestParam(value="distribution_media_type", required = false, defaultValue="text/csv")
                    String distributionMediaType
            , @RequestParam(value="distribution_encoding", required = false, defaultValue="UTF-8")
                    String distributionEncoding

            , @RequestParam(value="mapping_document_access_url", required = false) String mappingDocumentAccessURL
            , @RequestParam(value="mapping_document_download_url", required = false) String mappingDocumentDownloadURL
            , @RequestParam(value="mapping_document_file", required = false) MultipartFile mappingDocumentMultipartFile
            , @RequestParam(value="mapping_document_subject", required = false, defaultValue="") String mappingDocumentSubject
            , @RequestParam(value="mapping_document_title", required = false) String mappingDocumentTitle
            , @RequestParam(value="mapping_language", required = false, defaultValue="r2rml") String mappingLanguage

            , @RequestParam(value="execute_mapping", required = false, defaultValue="true") String executeMapping
            , @RequestParam(value="query_file_download_url", required = false) String queryFileDownloadURL
            , @RequestParam(value="output_file_name", required = false) String outputFilename
            , @RequestParam(value="output_fileextension", required = false) String outputFileExtension
            , @RequestParam(value="output_mediatype", required = false, defaultValue="text/txt") String outputMediaType

            , @RequestParam(value="manifestFile", required = false) MultipartFile manifestFileRef
            , @RequestParam(value="generateManifestFile", required = false, defaultValue="true") String pGenerateManifestFile

            , @RequestParam(value="use_cache", required = false, defaultValue="true") String pUseCache
            , @RequestParam(value="callback_url", required = false) String callbackURL
            , @RequestParam(value="callback_field", required = false) String callbackField
            , @RequestParam(value="update_resource", required = false, defaultValue="true") String pUpdateResource
    )
    {
        try {
            logger.info("[POST] /datasets_mappings_execute");
            boolean generateManifestFile = MPEUtility.stringToBoolean(pGenerateManifestFile);

            Agent organization = new Agent(organizationID);

            Dataset dataset = new Dataset(organization);
            if(datasetTitle == null) {
                dataset.dctTitle_$eq(dataset.dctIdentifier());
            } else {
                dataset.dctTitle_$eq(datasetTitle);
            }
            if(datasetDescription == null) {
                dataset.dctDescription_$eq(dataset.dctIdentifier());
            } else {
                dataset.dctDescription_$eq(datasetDescription);
            }
            dataset.dcatKeyword_$eq(datasetKeywords);
            dataset.dctLanguage_$eq(datasetLanguage);

            UnannotatedDistribution unannotatedDistribution = null;
            if(distributionDownloadURL != null ||  distributionMultipartFile != null) {
                unannotatedDistribution = new UnannotatedDistribution(dataset);

                if(distributionAccessURL == null) {
                    unannotatedDistribution.dcatAccessURL_$eq(distributionDownloadURL);
                } else {
                    unannotatedDistribution.dcatAccessURL_$eq(distributionAccessURL);
                }
                unannotatedDistribution.dcatDownloadURL_$eq(distributionDownloadURL);

                if(distributionMultipartFile != null) {
                    unannotatedDistribution.distributionFile_$eq(MpcUtility.multipartFileToFile(
                            distributionMultipartFile , dataset.dctIdentifier()));
                }

                unannotatedDistribution.dctDescription_$eq("Distribution for the dataset: " + dataset.dctIdentifier());
                unannotatedDistribution.dcatMediaType_$eq(distributionMediaType);
                unannotatedDistribution.encoding_$eq(distributionEncoding);
                dataset.addDistribution(unannotatedDistribution);
            }


//            AddDatasetResult addDatasetResult = this.datasetController.add(
//                    dataset, manifestFileRef, generateManifestFile, true);
//            int addDatasetResultStatusCode = addDatasetResult.getStatus_code();
            String datasetsServerUrl = MPCConstants.ENGINE_DATASETS_SERVER() + "datasets";
            logger.info("datasetsServerUrl = " + datasetsServerUrl);
            String postDatasetURL = datasetsServerUrl + "/" +  organizationID;
            logger.info("postDatasetURL = " + postDatasetURL);
            HttpResponse postDatasetsResponse = Unirest.post(postDatasetURL)
                    .field("dataset_id", dataset.dctIdentifier())
                    .field("dataset_title", dataset.dctTitle())
                    .field("dataset_keywords", dataset.dcatKeyword())
                    .field("dataset_language", dataset.dctLanguage())
                    .field("dataset_description", dataset.dctDescription())

                    .field("distribution_access_url", unannotatedDistribution.dcatAccessURL())
                    .field("distribution_download_url", unannotatedDistribution.dcatDownloadURL())
                    .field("distribution_file", unannotatedDistribution.distributionFile())
                    .field("distributionMediaType", unannotatedDistribution.dcatMediaType())
                    .field("distribution_encoding", unannotatedDistribution.encoding())

                    .field("manifestFile", manifestFileRef)
                    .field("generateManifestFile", pGenerateManifestFile)
                    .asJson();
            int addDatasetResultStatusCode = postDatasetsResponse.getStatus();




            if(addDatasetResultStatusCode >= 200 && addDatasetResultStatusCode < 300) {
                MappingDocument mappingDocument = new MappingDocument();
                mappingDocument.dctSubject_$eq(mappingDocumentSubject);
                mappingDocument.dctCreator_$eq(organizationID);
                mappingDocument.accessURL_$eq(mappingDocumentAccessURL);
                if(mappingDocumentTitle == null) {
                    mappingDocument.dctTitle_$eq(dataset.dctIdentifier());
                } else {
                    mappingDocument.dctTitle_$eq(mappingDocumentTitle);
                }
                mappingDocument.mappingLanguage_$eq(mappingLanguage);
                if(mappingDocumentMultipartFile != null) {
                    File mappingDocumentFile = MpcUtility.multipartFileToFile(
                            mappingDocumentMultipartFile, dataset.dctIdentifier());
                    mappingDocument.mappingDocumentFile_$eq(mappingDocumentFile);
                }

                mappingDocument.setDownloadURL(mappingDocumentDownloadURL);


//                AddMappingDocumentResult addMappingDocumentResult = mappingDocumentController.addNewMappingDocument(
//                        dataset, manifestFileRef, "true", generateManifestFile, mappingDocument);
//                int addMappingDocumentResultStatusCode = addMappingDocumentResult.getStatus_code();
                String mpeMappingsURL = MPCConstants.ENGINE_MAPPINGS_SERVER() + "mappings";
                logger.info("mpeMappingsURL = " + mpeMappingsURL);
                String postMappingsURL = mpeMappingsURL + "/" +  organizationID + "/" + dataset.dctIdentifier();
                logger.info("postMappingsURL = " + postMappingsURL);
                HttpResponse postMappingsResponse = Unirest.post(postMappingsURL)
                        .field("mapping_document_file", mappingDocument.mappingDocumentFile())
                        .field("mapping_document_download_url", mappingDocument.getDownloadURL())
                        .field("mappingDocumentSubjects", mappingDocument.dctSubject())
                        .field("mappingDocumentCreator", mappingDocument.dctCreator())
                        .field("mappingDocumentTitle", mappingDocument.dctTitle())
                        .field("mapping_language", mappingDocument.mappingLanguage())
                        .asJson();
                int addMappingDocumentResultStatusCode = postMappingsResponse.getStatus();

                boolean useCache = MPEUtility.stringToBoolean(pUseCache);
                if("true".equalsIgnoreCase(executeMapping)) {
                    if(addMappingDocumentResultStatusCode >= 200 && addMappingDocumentResultStatusCode < 300) {
                        boolean updateResource = MPEUtility.stringToBoolean(pUpdateResource);

                        try {
                            MappingExecution mappingExecution = new MappingExecution(
                                    //mappingDocument
                                    dataset.getUnannotatedDistributions()
                                    , null, queryFileDownloadURL
                                    , outputFilename, outputFileExtension, outputMediaType
                                    //, true
                                    , true
                                    , true
                                    , useCache
                                    , callbackURL
                                    , updateResource
                                    , mappingDocument.dctIdentifier()
                                    , mappingDocument.hash()
                                    , mappingDocument.getDownloadURL()
                                    , mappingDocument.getMapping_language()
                            );
                            mappingExecution.storeToCKAN_$eq(true);

                            ExecuteMappingResult executeMappingResult =
                                    this.mappingExecutionController.executeMapping(
                                            mappingExecution);

                            return new AddDatasetMappingExecuteResult (HttpURLConnection.HTTP_OK
//                                    , addDatasetResult, addMappingDocumentResult
                                    , executeMappingResult
                            );



                        } catch (Exception e){
                            e.printStackTrace();
                            return new AddDatasetMappingExecuteResult (HttpURLConnection.HTTP_INTERNAL_ERROR
//                                    , addDatasetResult, addMappingDocumentResult
                                    , null);

                        }
                    } else {
                        return new AddDatasetMappingExecuteResult(HttpURLConnection.HTTP_INTERNAL_ERROR
//                                , addDatasetResult, addMappingDocumentResult
                                , null);
                    }
                } else {
                    return new AddDatasetMappingExecuteResult(HttpURLConnection.HTTP_INTERNAL_ERROR
//                            ,addDatasetResult, addMappingDocumentResult
                            , null);
                }

            } else {
                return new AddDatasetMappingExecuteResult(HttpURLConnection.HTTP_INTERNAL_ERROR
//                        , addDatasetResult, null
                        , null);
            }
        } catch (Exception e) {
            e.printStackTrace();
            return new AddDatasetMappingExecuteResult(HttpURLConnection.HTTP_INTERNAL_ERROR
//                    , null, null
                    , null);
        }

    }




    @RequestMapping(value = "/queries/{organizationId}/{datasetId}", method= RequestMethod.POST)
    public GeneralResult postQueries(
            @RequestParam("queryFile") MultipartFile queryFileRef
            , @PathVariable("organizationId") String organizationId
            , @PathVariable("datasetId") String datasetId
    )
    {
        logger.info("[POST] /queries/{mappingpediaUsername}/{datasetID}");
        File queryFile = MpcUtility.multipartFileToFile(queryFileRef);
        return mappingExecutionController.addQueryFile(queryFile, organizationId, datasetId);
    }


    @RequestMapping(value="/ogd/utility/subclasses", method= RequestMethod.GET)
    public ListResult getSubclassesDetails(
            @RequestParam(value="aClass") String aClass
    ) {
        logger.info("GET /ogd/utility/subclasses ...");
        logger.info("aClass = " + aClass);
        ListResult result = mappingExecutionController.jenaClient().getSchemaOrgSubclassesDetail(
                aClass) ;
        //logger.info("result = " + result);
        return result;
    }

    @RequestMapping(value="/ogd/utility/subclassesSummary", method= RequestMethod.GET)
    public ListResult getSubclassesSummary(
            @RequestParam(value="aClass") String aClass
    ) {
        logger.info("GET /ogd/utility/subclassesSummary ...");
        logger.info("aClass = " + aClass);
        ListResult result = mappingExecutionController.jenaClient().getSubclassesSummary(aClass) ;
        //logger.info("result = " + result);
        return result;
    }

    @RequestMapping(value="/ogd/utility/superclassesSummary", method= RequestMethod.GET)
    public ListResult getSuperclassesSummary(@RequestParam(value="aClass") String aClass) {
        logger.info("GET /ogd/utility/superclassesSummary ...");
        logger.info("aClass = " + aClass);
        ListResult result = jenaClient.getSuperclasses(aClass);
        return result;
    }

    @RequestMapping(value="/ogd/instances", method= RequestMethod.GET)
    public ListResult getOGDInstances(@RequestParam(value="aClass") String aClass
            ,@RequestParam(value="maximum_results", defaultValue = "2") String pMaxMappingDocuments
            ,@RequestParam(value="use_cache", defaultValue = "true") String pUseCache

    ) {
        logger.info("GET /ogd/instances ...");
        logger.info("Getting instances of the class:" + aClass);
        logger.info("pMaxMappingDocuments = " + pMaxMappingDocuments);
        logger.info("use_cache = " + pUseCache);


        int maxMappingDocuments = 2;
        boolean useCache = true;
        try {
            maxMappingDocuments = Integer.parseInt(pMaxMappingDocuments);
            useCache = MPEUtility.stringToBoolean(pUseCache);

        } catch (Exception e) {
            logger.error("invalid value for maximum_mapping_documents!");
        }
        ListResult result = mappingExecutionController.getInstances(
                aClass, maxMappingDocuments, useCache, false) ;
        return result;
    }

    @RequestMapping(value="/instances", method= RequestMethod.GET)
    public ListResult getInstances(
            @RequestParam(value="class_name", defaultValue = "") String className
    ) {
        logger.info("GET /instances ...");
        logger.info("class_name = " + className);
        ListResult result = mappingExecutionController.findInstancesByClass(className) ;
        //logger.info("result = " + result);
        return result;
    }

}