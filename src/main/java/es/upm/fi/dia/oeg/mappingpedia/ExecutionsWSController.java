package es.upm.fi.dia.oeg.mappingpedia;

import java.io.File;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import javax.servlet.annotation.MultipartConfig;


import es.upm.fi.dia.oeg.mappingpedia.controller.MappingExecutionController;
import es.upm.fi.dia.oeg.mappingpedia.model.*;
import es.upm.fi.dia.oeg.mappingpedia.model.result.*;
import es.upm.fi.dia.oeg.mappingpedia.utility.*;
import org.apache.commons.io.FileUtils;
//import org.apache.jena.ontology.OntModel;
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


    //private OntModel ontModel = MappingPediaEngine.ontologyModel();


//    private GitHubUtility githubClient = MappingPediaEngine.githubClient();
//    private CKANUtility ckanClient = MappingPediaEngine.ckanClient();
//    private JenaClient jenaClient = MappingPediaEngine.jenaClient();
//    private VirtuosoClient virtuosoClient = MappingPediaEngine.virtuosoClient();

    //private MappingExecutionController mappingExecutionController= new MappingExecutionController(ckanClient, githubClient, virtuosoClient, jenaClient);
    private MappingExecutionController mappingExecutionController = MappingExecutionController.apply();
    private MPCJenaUtility jenaClient = mappingExecutionController.jenaClient();
    private CKANUtility ckanClient = mappingExecutionController.ckanClient();

    @RequestMapping(value="/greeting", method= RequestMethod.GET)
    public GreetingJava getGreeting(@RequestParam(value="name", defaultValue="World") String name) {
        logger.info("/greeting(GET) ...");
        return new GreetingJava(counter.incrementAndGet(),
                String.format(template, name));
    }

    @RequestMapping(value="/", method= RequestMethod.GET, produces={"application/ld+json"})
    public Inbox get() {
        logger.info("GET / ...");
        return new Inbox();
    }

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
        String uri = MappingPediaUtility.getClassURI(resource);

        return this.jenaClient.getDetails(uri);
    }

    @RequestMapping(value="/github_repo_url", method= RequestMethod.GET)
    public String getGitHubRepoURL() {
        logger.info("GET /github_repo_url ...");
        return MappingPediaEngine.mappingpediaProperties().githubRepository();
    }

    @RequestMapping(value="/ckan_datasets", method= RequestMethod.GET)
    public ListResult getCKANDatasets(@RequestParam(value="catalogUrl", required = false) String catalogUrl) {
        if(catalogUrl == null) {
            catalogUrl = MappingPediaEngine.mappingpediaProperties().ckanURL();
        }
        logger.info("GET /ckanDatasetList ...");
        return CKANUtility.getDatasetList(catalogUrl);
    }

    @RequestMapping(value="/virtuoso_enabled", method= RequestMethod.GET)
    public String getVirtuosoEnabled() {
        logger.info("GET /virtuosoEnabled ...");
        return MappingPediaEngine.mappingpediaProperties().virtuosoEnabled() + "";
    }

    @RequestMapping(value="/mappingpedia_graph", method= RequestMethod.GET)
    public String getMappingpediaGraph() {
        logger.info("/getMappingPediaGraph(GET) ...");
        return MappingPediaEngine.mappingpediaProperties().graphName();
    }

    @RequestMapping(value="/ckan_api_action_organization_create", method= RequestMethod.GET)
    public String getCKANAPIActionOrganizationCreate() {
        logger.info("GET /ckanActionOrganizationCreate ...");
        return MappingPediaEngine.mappingpediaProperties().ckanActionOrganizationCreate();
    }

    @RequestMapping(value="/ckan_api_action_package_create", method= RequestMethod.GET)
    public String getCKANAPIActionPpackageCreate() {
        logger.info("GET /ckanActionPackageCreate ...");
        return MappingPediaEngine.mappingpediaProperties().ckanActionPackageCreate();
    }

    @RequestMapping(value="/ckan_api_action_resource_create", method= RequestMethod.GET)
    public String getCKANAPIActionResourceCreate() {
        logger.info("GET /getCKANActionResourceCreate ...");
        return MappingPediaEngine.mappingpediaProperties().ckanActionResourceCreate();
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
        String ckanURL = MappingPediaEngine.mappingpediaProperties().ckanURL();
        String ckanKey = MappingPediaEngine.mappingpediaProperties().ckanKey();

        CKANUtility ckanClient = new CKANUtility(ckanURL, ckanKey);
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
        String ckanURL = MappingPediaEngine.mappingpediaProperties().ckanURL();
        String ckanKey = MappingPediaEngine.mappingpediaProperties().ckanKey();

        CKANUtility ckanClient = new CKANUtility(ckanURL, ckanKey);
        return ckanClient.updateDatasetLanguage(organizationId, datasetLanguage);
    }

    @RequestMapping(value="/triples_maps", method= RequestMethod.GET)
    public ListResult getTriplesMaps() {
        logger.info("/triplesMaps ...");
        ListResult listResult = MappingPediaEngine.getAllTriplesMaps();
        //logger.info("listResult = " + listResult);

        return listResult;
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
            , @RequestParam(value="ckan_package_id", required = false) String ckanPackageId
            , @RequestParam(value="ckan_package_name", required = false) String ckanPackageName

            //Distribution related fields
            , @RequestParam(value="ckan_resources_ids", required = false) String ckanResourcesIds
            , @RequestParam(value="distribution_access_url", required = false) String distributionAccessURL
            , @RequestParam(value="distribution_download_url", required = false) String pDistributionDownloadURL
            , @RequestParam(value="distribution_mediatype", required = false, defaultValue="text/csv") String distributionMediaType
            , @RequestParam(value="distribution_encoding", required = false, defaultValue="UTF-8") String distributionEncoding
            , @RequestParam(value="field_separator", required = false) String fieldSeparator

            //Mapping document related fields
            , @RequestParam(value="mapping_document_id", required = false) String mappingDocumentId
            , @RequestParam(value="mapping_document_download_url", required = false) String pMappingDocumentDownloadURL
            , @RequestParam(value="mapping_language", required = false) String pMappingLanguage
            , @RequestParam(value="use_cache", required = false) String pUseCache
            , @RequestParam(value="callback_url", required = false) String callbackURL

            //Execution related field
            , @RequestParam(value="query_file", required = false) String queryFile
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
        logger.info("ckan_package_id = " + ckanPackageId);
        logger.info("ckan_package_name = " + ckanPackageName);
        logger.info("distributionDownloadURL = " + pDistributionDownloadURL);
        logger.info("ckan_resources_ids = " + ckanResourcesIds);
        logger.info("mapping_document_id = " + mappingDocumentId);
        logger.info("mappingDocumentDownloadURL = " + pMappingDocumentDownloadURL);
        logger.info("distribution_encoding = " + distributionEncoding);
        logger.info("use_cache = " + pUseCache);
        logger.info("output_filename = " + outputFilename);
        logger.info("output_fileextension = " + outputFileExtension);
        logger.info("output_mediatype = " + outputMediaType);
        logger.info("callback_url = " + callbackURL);
        logger.info("pUpdateResource = " + pUpdateResource);



        try {
            Agent organization = Agent.apply(organizationId);

            Dataset dataset = this.datasetController.findOrCreate(
                    organizationId, pDatasetId, ckanPackageId, ckanPackageName);
            logger.info("dataset.dctIdentifier() = " + dataset.dctIdentifier());
            logger.info("dataset.ckanPackageId = " + dataset.ckanPackageId());

            //List<String> listDistributionDownloadURLs = null;
            //String[] arrayDistributionDownloadURLs = null;
            //List<UnannotatedDistribution> unannotatedDistributions = new ArrayList<UnannotatedDistribution>();
            if(ckanResourcesIds != null) {
                List<String> listCKANResourcesIds = Arrays.asList(ckanResourcesIds.split(","));
                logger.info("listCKANResourcesIds = " + listCKANResourcesIds);

                for (String resourceId:listCKANResourcesIds) {
                    UnannotatedDistribution unannotatedDistribution = new UnannotatedDistribution(dataset);
                    unannotatedDistribution.ckanResourceId_$eq(resourceId);
                    String ckanResourceDownloadUrl = this.ckanClient.getResourcesUrlsAsJava(resourceId).iterator().next();
                    unannotatedDistribution.dcatDownloadURL_$eq(ckanResourceDownloadUrl);
                    if(fieldSeparator != null) {
                        unannotatedDistribution.csvFieldSeparator_$eq(fieldSeparator);
                    }
                    unannotatedDistribution.dcatMediaType_$eq(distributionMediaType);

                    dataset.addDistribution(unannotatedDistribution);
                }
            } else if(pDistributionDownloadURL != null) {
                List<String> listDistributionDownloadURLs = Arrays.asList(pDistributionDownloadURL.split(","));
                logger.info("listDistributionDownloadURLs = " + listDistributionDownloadURLs);
                for(String distributionDownloadURL:listDistributionDownloadURLs) {
                    UnannotatedDistribution unannotatedDistribution = new UnannotatedDistribution(dataset);
                    String distributionDownloadURLTrimmed = distributionDownloadURL.trim();
                    String resourceId = this.ckanClient.getResourceIdByResourceUrl(dataset.ckanPackageId(), distributionDownloadURLTrimmed);
                    unannotatedDistribution.ckanResourceId_$eq(resourceId);
                    unannotatedDistribution.dcatDownloadURL_$eq(distributionDownloadURLTrimmed);
                    if(fieldSeparator != null) {
                        unannotatedDistribution.csvFieldSeparator_$eq(fieldSeparator);
                    }
                    unannotatedDistribution.dcatMediaType_$eq(distributionMediaType);

                    dataset.addDistribution(unannotatedDistribution );
                }
            }

            MappingDocument md = this.mappingDocumentController.findOrCreate(
                    mappingDocumentId);
            logger.info("md.dctIdentifier() = " + md.dctIdentifier());

            if(pMappingDocumentDownloadURL != null) {
                md.setDownloadURL(pMappingDocumentDownloadURL);
            }
            logger.info("md.getDownloadURL() = " + md.getDownloadURL());
            String mdDownloadURL = md.getDownloadURL();

            if(md.hash() == null && mdDownloadURL != null) {
                md.hash_$eq(MappingPediaUtility.calculateHash(
                        mdDownloadURL, "UTF-8"));
            }
            logger.debug("md.sha = " + md.hash());

            if(pMappingLanguage != null) {
                md.mappingLanguage_$eq(pMappingLanguage);
            } else if(md.mappingLanguage() == null){
                String mappingLanguage = MappingDocumentController.detectMappingLanguage(
                        mdDownloadURL);
                md.mappingLanguage_$eq(mappingLanguage);
            }
            logger.debug("md.getMapping_language() = " + md.getMapping_language());





            JDBCConnection jdbcConnection = null;
            if(dbUserName != null && dbPassword != null && dbName != null
                    && jdbcURL != null && databaseDriver != null && databaseType != null) {
                jdbcConnection = new JDBCConnection(dbUserName, dbPassword
                        , dbName, jdbcURL
                        , databaseDriver, databaseType);
            }


            Boolean useCache = MappingPediaUtility.stringToBoolean(pUseCache);
            Boolean updateResource = MappingPediaUtility.stringToBoolean(pUpdateResource);
            MappingExecution mappingExecution = new MappingExecution(md
                    , dataset.getUnannotatedDistributions()
                    , jdbcConnection
                    , queryFile
                    , outputFilename
                    , outputFileExtension
                    , outputMediaType
                    , true
                    , true
                    , true
                    , useCache
                    , callbackURL
                    , updateResource
            );
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







    @RequestMapping(value="/mappings/{mappingpediaUsername}/{mappingDirectory}/{mappingFilename:.+}", method= RequestMethod.GET)
    public GeneralResult getMapping(
            @PathVariable("mappingpediaUsername") String mappingpediaUsername
            , @PathVariable("mappingDirectory") String mappingDirectory
            , @PathVariable("mappingFilename") String mappingFilename
    )
    {
        logger.info("GET /mappings/{mappingpediaUsername}/{mappingDirectory}/{mappingFilename}");
        return MappingPediaEngine.getMapping(mappingpediaUsername, mappingDirectory, mappingFilename);
    }


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
            , @RequestParam(value="distribution_media_type", required = false, defaultValue="text/csv") String distributionMediaType
            , @RequestParam(value="distribution_encoding", required = false, defaultValue="UTF-8") String distributionEncoding

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
        logger.info("[POST] /datasets_mappings_execute");
        boolean generateManifestFile = MappingPediaUtility.stringToBoolean(pGenerateManifestFile);

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
                unannotatedDistribution.distributionFile_$eq(MappingPediaUtility.multipartFileToFile(
                        distributionMultipartFile , dataset.dctIdentifier()));
            }

            unannotatedDistribution.dctDescription_$eq("Distribution for the dataset: " + dataset.dctIdentifier());
            unannotatedDistribution.dcatMediaType_$eq(distributionMediaType);
            unannotatedDistribution.encoding_$eq(distributionEncoding);
            dataset.addDistribution(unannotatedDistribution);
        }


        AddDatasetResult addDatasetResult = this.datasetController.add(
                dataset, manifestFileRef, generateManifestFile, true);
        int addDatasetResultStatusCode = addDatasetResult.getStatus_code();
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
                File mappingDocumentFile = MappingPediaUtility.multipartFileToFile(mappingDocumentMultipartFile, dataset.dctIdentifier());
                mappingDocument.mappingDocumentFile_$eq(mappingDocumentFile);
            }

            mappingDocument.setDownloadURL(mappingDocumentDownloadURL);


            AddMappingDocumentResult addMappingDocumentResult = mappingDocumentController.addNewMappingDocument(dataset, manifestFileRef
                    , "true", generateManifestFile, mappingDocument);
            int addMappingDocumentResultStatusCode = addMappingDocumentResult.getStatus_code();

            boolean useCache = MappingPediaUtility.stringToBoolean(pUseCache);
            if("true".equalsIgnoreCase(executeMapping)) {
                if(addMappingDocumentResultStatusCode >= 200 && addMappingDocumentResultStatusCode < 300) {
                    boolean updateResource = MappingPediaUtility.stringToBoolean(pUpdateResource);

                    try {
                        MappingExecution mappingExecution = new MappingExecution(
                                mappingDocument, dataset.getUnannotatedDistributions()
                                , null, queryFileDownloadURL
                                , outputFilename, outputFileExtension, outputMediaType
                                , true
                                , true
                                , true
                                , useCache
                                , callbackURL
                                , updateResource

                        );

                        ExecuteMappingResult executeMappingResult =
                                this.mappingExecutionController.executeMapping(
                                        mappingExecution);

                        return new AddDatasetMappingExecuteResult (HttpURLConnection.HTTP_OK, addDatasetResult, addMappingDocumentResult, executeMappingResult);



                    } catch (Exception e){
                        e.printStackTrace();
                        return new AddDatasetMappingExecuteResult (HttpURLConnection.HTTP_INTERNAL_ERROR, addDatasetResult, addMappingDocumentResult, null);

                    }
                } else {
                    return new AddDatasetMappingExecuteResult(HttpURLConnection.HTTP_INTERNAL_ERROR, addDatasetResult, addMappingDocumentResult, null);
                }
            } else {
                return new AddDatasetMappingExecuteResult(HttpURLConnection.HTTP_INTERNAL_ERROR,addDatasetResult, addMappingDocumentResult, null);
            }

        } else {
            return new AddDatasetMappingExecuteResult(HttpURLConnection.HTTP_INTERNAL_ERROR, addDatasetResult, null, null);
        }
    }




    @RequestMapping(value = "/queries/{mappingpediaUsername}/{datasetID}", method= RequestMethod.POST)
    public GeneralResult postQueries(
            @RequestParam("queryFile") MultipartFile queryFileRef
            , @PathVariable("mappingpediaUsername") String mappingpediaUsername
            , @PathVariable("datasetID") String datasetID
    )
    {
        logger.info("[POST] /queries/{mappingpediaUsername}/{datasetID}");
        return MappingPediaEngine.addQueryFile(queryFileRef, mappingpediaUsername, datasetID);
    }


    @RequestMapping(value = "/rdf_file", method= RequestMethod.POST)
    public GeneralResult postRDFFile(
            @RequestParam("rdfFile") MultipartFile fileRef
            , @RequestParam(value="graphURI") String graphURI)
    {
        logger.info("/storeRDFFile...");
        return MappingPediaEngine.storeRDFFile(fileRef, graphURI);
    }

    @RequestMapping(value="/ogd/utility/subclasses", method= RequestMethod.GET)
    public ListResult getSubclassesDetails(
            @RequestParam(value="aClass") String aClass
    ) {
        logger.info("GET /ogd/utility/subclasses ...");
        logger.info("aClass = " + aClass);
        ListResult result = MappingPediaEngine.getSchemaOrgSubclassesDetail(aClass) ;
        //logger.info("result = " + result);
        return result;
    }

    @RequestMapping(value="/ogd/utility/subclassesSummary", method= RequestMethod.GET)
    public ListResult getSubclassesSummary(
            @RequestParam(value="aClass") String aClass
    ) {
        logger.info("GET /ogd/utility/subclassesSummary ...");
        logger.info("aClass = " + aClass);
        ListResult result = MappingPediaEngine.getSubclassesSummary(aClass) ;
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
            useCache = MappingPediaUtility.stringToBoolean(pUseCache);

        } catch (Exception e) {
            logger.error("invalid value for maximum_mapping_documents!");
        }
        ListResult result = mappingExecutionController.getInstances(
                aClass, maxMappingDocuments, useCache, false) ;
        return result;
    }

}