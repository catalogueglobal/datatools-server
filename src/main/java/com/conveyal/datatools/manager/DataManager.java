package com.conveyal.datatools.manager;

import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.conveyal.datatools.common.utils.SparkUtils;
import com.conveyal.datatools.manager.auth.Auth0Connection;

import com.conveyal.datatools.manager.controllers.DumpController;
import com.conveyal.datatools.manager.controllers.api.*;
import com.conveyal.datatools.editor.controllers.api.*;

import com.conveyal.datatools.manager.extensions.ExternalFeedResource;
import com.conveyal.datatools.manager.extensions.mtc.MtcFeedResource;
import com.conveyal.datatools.manager.extensions.transitfeeds.TransitFeedsFeedResource;
import com.conveyal.datatools.manager.extensions.transitland.TransitLandFeedResource;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.jobs.FeedUpdater;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.FeedStore;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.utils.CorsFilter;
import com.conveyal.gtfs.GTFS;
import com.conveyal.gtfs.GraphQLMain;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.io.Resources;
import org.apache.commons.io.Charsets;
import org.eclipse.jetty.util.ConcurrentHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.utils.IOUtils;

import javax.sql.DataSource;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import static com.conveyal.datatools.common.utils.SparkUtils.haltWithError;
import static spark.Spark.*;

public class DataManager {

    public static final Logger LOG = LoggerFactory.getLogger(DataManager.class);

    public static JsonNode config;
    public static JsonNode serverConfig;

    public static JsonNode gtfsPlusConfig;
    public static JsonNode gtfsConfig;

    // TODO: define type for ExternalFeedResource Strings
    public static final Map<String, ExternalFeedResource> feedResources = new HashMap<>();

    public static Map<String, ConcurrentHashSet<MonitorableJob>> userJobsMap = new ConcurrentHashMap<>();

    public static Map<String, ScheduledFuture> autoFetchMap = new HashMap<>();
    public final static ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private static final ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());


    // heavy executor should contain long-lived CPU-intensive tasks (e.g., feed loading/validation)
    public static Executor heavyExecutor = Executors.newFixedThreadPool(4); // Runtime.getRuntime().availableProcessors()
    // light executor is for tasks for things that should finish quickly (e.g., email notifications)
    public static Executor lightExecutor = Executors.newSingleThreadExecutor();

    public static File storageDirectory;

    // S3 connection/configuration fields
    public static boolean useS3;
    public static AmazonS3 s3Client;
    public static String feedBucket;
    private static String awsRole;
    public static final String GTFS_S3_PREFIX = "gtfs/";
    private static final String GTFSPLUS = "gtfsplus";
    private static final String PROJECT = "project";
    private static final String SNAPSHOTS = "snapshots";

    // String constants used for API configuration
    private static final String API_PREFIX = "/api/manager/";
    // TODO: move gtfs-api routes to gtfs path and add auth
    private static final String GTFS_API_PREFIX = API_PREFIX; // + "gtfs/";
    public static final String EDITOR_API_PREFIX = "/api/editor/";
    // Regex that checks whether a "public" (i.e., non-authenticated) API path has been used on the API request.
    public static final String publicPath = "(" + DataManager.API_PREFIX + "|" + DataManager.EDITOR_API_PREFIX + ")public/.*";

    // String constants used for application configuration
    private static final String DEFAULT_ENV = "configurations/default/env.yml";
    private static final String DEFAULT_CONFIG = "configurations/default/server.yml";
    // FIXME: move this out of FeedVersion (also, it should probably not be public)?
    public static FeedStore feedStore;
    public static FeedStore projectStore;
    private static FeedStore gtfsPlusStore;
    private static FeedStore snapshotStore;
    private static FeedUpdater feedUpdater;

    // Database connection and persistence fields
    public static DataSource GTFS_DATA_SOURCE;
    private static String extensionType;
    // TODO: Make Persistence operate non-statically?

    public static void main(String[] args) throws IOException {

        // load config
        loadConfig(args);

        // Optionally set port for server. Otherwise, Spark defaults to 4000.
        String port = getConfigPropertyAsText("application.port");
        if (port != null) {
            port(Integer.parseInt(port));
        }
        // Set up storage objects, including GTFS file storage, MongoDB connection, and PostgreSQL connection.
        setUpPersistence();

        // These must come after persistence setup because they use static persistence fields
        // (i.e., storageDirectory, extensionType, s3Client, awsRole).
        feedUpdater = createFeedUpdater(storageDirectory, extensionType, s3Client, awsRole);
        scheduleProjectAutoFeedFetch(autoFetchMap);


        // Initialize GTFS GraphQL API service
        GraphQLMain.initialize(GTFS_DATA_SOURCE, API_PREFIX);
        LOG.info("Initialized gtfs-api at localhost:port{}", API_PREFIX);
        registerAPIRoutes();

        registerExternalFeedResources();

    }
    //    public static Persistence persistence;

    /**
     * Create FeedUpdater which currently syncs feeds from S3 (for MTC), so that the latest FeedSource is used in the
     * alerts and GTFS+ modules when fetching GTFS entities.
     * FIXME: This should instead of downloading feeds from s3, just check the Etags and update MongoDB with a processed
     * feed version field. However, do we want the client to be able to arbitrarily request entities without an additional
     * id (currently it can just get entities with the feed source ID)?
     */
    private static FeedUpdater createFeedUpdater(File storageDirectory, String extensionType, AmazonS3 s3Client, String awsRole) {
        // check for use of extension...// store list of GTFS feed eTags here Map<FeedId, eTag value>
        String bucketFolder, feedUpdaterBucket;
        switch (extensionType) {
            case "mtc":
                LOG.info("Using extension " + extensionType + " for service alerts module");
                feedUpdaterBucket = DataManager.getConfigPropertyAsText("extensions." + extensionType + ".s3_bucket");
                bucketFolder = DataManager.getConfigPropertyAsText("extensions." + extensionType + ".s3_download_prefix");
                // check for update interval (in seconds) and initialize feedUpdater
                JsonNode updateFrequency = DataManager.getConfigProperty("modules.gtfsapi.update_frequency");
                if (updateFrequency != null) {
                    if (feedUpdaterBucket != null) {
                        FeedStore updaterStore = new FeedStore(storageDirectory, null, bucketFolder, feedUpdaterBucket, s3Client, awsRole);
                        // Create and start feedUpdater instantly
                        FeedUpdater feedUpdater = new FeedUpdater(updaterStore, 0, updateFrequency.asInt());
                        return feedUpdater;
                    } else {
                        LOG.warn("FeedUpdater not initialized. No s3 bucket provided (or use_s3_storage set to false).");
                    }
                }
                // Adds feeds on startup
                // FIXME: check that this already gets handled when FeedUpdater is constructed.
//                eTagMap.putAll(registerS3Feeds(null, feedUpdaterBucket, bucketFolder));
                break;
            default:
                LOG.info("No extension found for FeedUpdater. Feeds will not be regularly synced from s3.");
                break;
        }
        return null;
    }

    private static void scheduleProjectAutoFeedFetch(Map<String, ScheduledFuture> autoFetchMap) {
        // initialize map of auto fetched projects
        for (Project project : Persistence.projects.getAll()) {
            if (project.autoFetchFeeds) {
                ScheduledFuture scheduledFuture = ProjectController.scheduleAutoFeedFetch(project, 1);
                autoFetchMap.put(project.id, scheduledFuture);
            }
        }
    }

    private static void setUpPersistence() {
        // Create static data source for PostgreSQL database to store processed GTFS data.
        GTFS_DATA_SOURCE = GTFS.createDataSource(
                getConfigPropertyAsText("GTFS_DATABASE_URL"),
                getConfigPropertyAsText("GTFS_DATABASE_USER"),
                getConfigPropertyAsText("GTFS_DATABASE_PASSWORD")
        );
        // Set up S3 connection details
        useS3 = "true".equals(getConfigPropertyAsText("application.data.use_s3_storage"));
        feedBucket = getConfigPropertyAsText("application.data.gtfs_s3_bucket");
        awsRole = getConfigPropertyAsText("application.data.aws_role");
        // Determine extensionType for gtfs-api
        extensionType = DataManager.hasConfigProperty("modules.gtfsapi.use_extension")
                ? DataManager.getConfigPropertyAsText("modules.gtfsapi.use_extension")
                : "false";
        s3Client = initializeS3Client(useS3, extensionType);

        String gtfsUploadPath = getConfigPropertyAsText("application.data.gtfs");
        if (gtfsUploadPath == null) {
            throw new IllegalArgumentException("GTFS storage directory must be provided.");
        }
        storageDirectory = new File(gtfsUploadPath);
        // Primary storage for GTFS feed versions
        feedStore = new FeedStore(storageDirectory, null, GTFS_S3_PREFIX, feedBucket, s3Client, awsRole);
        // FeedStore in which to store merged project feeds.
        projectStore = new FeedStore(DataManager.storageDirectory, PROJECT, PROJECT, feedBucket, s3Client, awsRole);


        // Initialize the MongoDB connection and collections
        Persistence.initialize();
    }

    /**
     * Create AmazonS3 client for use in application.
     */
    private static AmazonS3 initializeS3Client(boolean useS3, String extensionType) {
        if (useS3 || "mtc".equals(extensionType)){
            // Only construct S3 client if useS3 config option set to true or if MTC module specified, which relies on
            // downloading and uploading feeds to S3 to exchange data with MTC's Regional Transit Database (RTD).
            AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard().withCredentials(getAWSCredentials());

            // If region configuration string is provided, use that. Otherwise, default to value contained within
            // ~/.aws/config (default builder behavior).
            // WARNING: s3_region value must match the region for s3 bucket provided in application.data.gtfs_s3_bucket
            String s3Region = DataManager.getConfigPropertyAsText("application.data.s3_region");
            if (s3Region != null) {
                LOG.info("Using S3 region {}", s3Region);
                builder.withRegion(s3Region);
            }
            try {
                return builder.build();
            } catch (SdkClientException e) {
                LOG.error("S3 client not initialized correctly.  Must provide config property application.data.s3_region or specify region in ~/.aws/config", e);
                throw new IllegalArgumentException("Fatal error initializing s3Bucket or s3Client");
            }
        } else {
            // If the application is not using S3, do not construct an S3 client.
            return null;
        }
    }

    /**
     * Register API routes with Spark. This register core application routes, any routes associated with optional
     * modules and sets other core routes (e.g., 404 response) and response headers (e.g., API content type is JSON).
     */
    private static void registerAPIRoutes() throws IOException {
        CorsFilter.apply();

        // core controllers
        ProjectController.register(API_PREFIX);
        FeedSourceController.register(API_PREFIX);
        FeedVersionController.register(API_PREFIX);
        RegionController.register(API_PREFIX);
        NoteController.register(API_PREFIX);
        StatusController.register(API_PREFIX);
        OrganizationController.register(API_PREFIX);

        // Editor routes
        if (isModuleEnabled("editor")) {
            snapshotStore = new FeedStore(storageDirectory, SNAPSHOTS, SNAPSHOTS, feedBucket, s3Client, awsRole);
            SnapshotController.register(EDITOR_API_PREFIX, snapshotStore);

            String gtfs = IOUtils.toString(DataManager.class.getResourceAsStream("/gtfs/gtfs.yml"));
            gtfsConfig = yamlMapper.readTree(gtfs);
            AgencyController.register(EDITOR_API_PREFIX);
            CalendarController.register(EDITOR_API_PREFIX);
            RouteController.register(EDITOR_API_PREFIX);
            RouteTypeController.register(EDITOR_API_PREFIX);
            ScheduleExceptionController.register(EDITOR_API_PREFIX);
            StopController.register(EDITOR_API_PREFIX);
            TripController.register(EDITOR_API_PREFIX);
            TripPatternController.register(EDITOR_API_PREFIX);
            FeedInfoController.register(EDITOR_API_PREFIX);
            FareController.register(EDITOR_API_PREFIX);
//            GisController.register(EDITOR_API_PREFIX);
        }

        // log all exceptions to system.out
        exception(Exception.class, (e, req, res) -> LOG.error("error", e));

        // module-specific controllers
        if (isModuleEnabled("deployment")) {
            DeploymentController.register(API_PREFIX);
        }
        if (isModuleEnabled("gtfsplus")) {
            // GtfsPlusController#register will statically initialize FeedStore field and ensure directory is writeable.
            gtfsPlusStore = new FeedStore(storageDirectory, GTFSPLUS, GTFSPLUS, feedBucket, s3Client, awsRole);
            URL gtfsPlus = DataManager.class.getResource("/gtfs/gtfsplus.yml");
            gtfsPlusConfig = yamlMapper.readTree(Resources.toString(gtfsPlus, Charsets.UTF_8));
            GtfsPlusController.register(API_PREFIX, gtfsPlusStore);
        }
        if (isModuleEnabled("user_admin")) {
            UserController.register(API_PREFIX);
        }
        if (isModuleEnabled("dump")) {
            DumpController.register("/");
        }

        before(EDITOR_API_PREFIX + "secure/*", ((request, response) -> {
            Auth0Connection.checkUser(request);
            Auth0Connection.checkEditPrivileges(request);
        }));

        before(API_PREFIX + "secure/*", (request, response) -> {
            if(request.requestMethod().equals("OPTIONS")) return;
            Auth0Connection.checkUser(request);
        });

        // FIXME: add auth check for gtfs-api. Should access to certain feeds be restricted by feedId or namespace?
//        before(GTFS_API_PREFIX + "*", (request, response) -> {
//            if(request.requestMethod().equals("OPTIONS")) return;
//            Auth0Connection.checkUser(request);
//        });
        final String publicUrl = getConfigPropertyAsText("application.public_url");
//        Auth0Connection.logRequest(publicUrl, EDITOR_API_PREFIX);
//        Auth0Connection.logRequest(publicUrl, API_PREFIX);

        // return 404 for any api response that's not found
        get(API_PREFIX + "*", (request, response) -> {
            halt(404, SparkUtils.formatJSON("Unknown error occurred.", 404));
            return null;
        });

        InputStream auth0Stream = DataManager.class.getResourceAsStream("/public/auth0-silent-callback.html");
        final String auth0html = IOUtils.toString(auth0Stream);
        auth0Stream.close();

        // auth0 silent callback
        get("/api/auth0-silent-callback", (request, response) -> {
            response.type("text/html");
            return auth0html;
        });

        // load index.html
        InputStream stream = DataManager.class.getResourceAsStream("/public/index.html");
        // Set index to null if missing application.assets_bucket config property
        final String index = hasConfigProperty("application.assets_bucket")
                ? IOUtils.toString(stream).replace("${S3BUCKET}", getConfigPropertyAsText("application.assets_bucket"))
                : null;
        stream.close();

        // Return index.html for any client response (outside of API prefix)
        get("/*", (request, response) -> {
            if (index != null) {
                response.type("text/html");
                return index;
            } else {
                // If index is null, assets_bucket config variable has not been provided. This should only be the case
                // if serving index.html statically somewhere besides this application's http server.
                response.type("application/json");
                haltWithError(400, "Server's application.assets_bucket not configured properly. Reconfigure or serve index.html statically.");
                return null;
            }
        });
    }

    /**
     * Convenience function to check existence of a config property (nested fields defined by dot notation
     * "data.use_s3_storage") without specifying server.yml or env.yml.
     */
    public static boolean hasConfigProperty(String name) {
        // try the server config first, then the main config
        return hasConfigPropertyInternal(serverConfig, name) || hasConfigPropertyInternal(config, name);
    }

    /**
     * Function to check a given config JsonNode for the specified property name (nested fields defined by dot notation
     * "data.use_s3_storage").
     */
    private static boolean hasConfigPropertyInternal(JsonNode config, String name) {
        String parts[] = name.split("\\.");
        JsonNode node = config;
        for (String part : parts) {
            if (node == null) return false;
            node = node.get(part);
        }
        return node != null;
    }

    /**
     * Convenience function to get a config property (nested fields defined by dot notation "data.use_s3_storage") as
     * JsonNode. Checks server.yml, then env.yml, and finally returns null if property is not found.
     */
    public static JsonNode getConfigProperty(String name) {
        // try the server config first, then the main config
        JsonNode fromServerConfig = getConfigProperty(serverConfig, name);
        if(fromServerConfig != null) return fromServerConfig;

        return getConfigProperty(config, name);
    }

    private static JsonNode getConfigProperty(JsonNode config, String name) {
        String parts[] = name.split("\\.");
        JsonNode node = config;
        for (String part : parts) {
            if (node == null) {
                LOG.warn("Config property {} not found", name);
                return null;
            }
            node = node.get(part);
        }
        return node;
    }

    /**
     * Get a config property (nested fields defined by dot notation "data.use_s3_storage") as text.
     */
    public static String getConfigPropertyAsText(String name) {
        JsonNode node = getConfigProperty(name);
        if (node != null) {
            return node.asText();
        } else {
            LOG.warn("Config property {} not found", name);
            return null;
        }
    }

    /**
     * Checks if an application module (e.g., editor, GTFS+) has been enabled. The UI must also have the module
     * enabled in order to use.
     */
    public static boolean isModuleEnabled(String moduleName) {
        return hasConfigProperty("modules." + moduleName) && "true".equals(getConfigPropertyAsText("modules." + moduleName + ".enabled"));
    }

    /**
     * Checks if an extension has been enabled. Extensions primarily define external resources
     * the application can sync with. The UI config must also have the extension enabled in order to use.
     */
    private static boolean isExtensionEnabled(String extensionName) {
        return hasConfigProperty("extensions." + extensionName) && "true".equals(getConfigPropertyAsText("extensions." + extensionName + ".enabled"));
    }

    /**
     * Check if extension is enabled and, if so, register it.
     */
    private static void registerExternalFeedResources() {

        if (isExtensionEnabled("mtc")) {
            LOG.info("Registering MTC Resource");
            registerExternalResource(new MtcFeedResource(s3Client, awsRole));
        }

        if (isExtensionEnabled("transitland")) {
            LOG.info("Registering TransitLand Resource");
            registerExternalResource(new TransitLandFeedResource());
        }

        if (isExtensionEnabled("transitfeeds")) {
            LOG.info("Registering TransitFeeds Resource");
            registerExternalResource(new TransitFeedsFeedResource());
        }
    }

    /**
     * Load config files from either program arguments or (if no args specified) from
     * default configuration file locations. Config fields are retrieved with getConfigProperty.
     */
    private static void loadConfig(String[] args) throws IOException {
        FileInputStream configStream;
        FileInputStream serverConfigStream;

        if (args.length == 0) {
            LOG.warn("Using default env.yml: {}", DEFAULT_ENV);
            LOG.warn("Using default server.yml: {}", DEFAULT_CONFIG);
            configStream = new FileInputStream(new File(DEFAULT_ENV));
            serverConfigStream = new FileInputStream(new File(DEFAULT_CONFIG));
        }
        else {
            LOG.info("Loading env.yml: {}", args[0]);
            LOG.info("Loading server.yml: {}", args[1]);
            configStream = new FileInputStream(new File(args[0]));
            serverConfigStream = new FileInputStream(new File(args[1]));
        }

        config = yamlMapper.readTree(configStream);
        serverConfig = yamlMapper.readTree(serverConfigStream);
    }

    /**
     * Register external feed resource (e.g., transit.land) with feedResources map.
     * This essentially "enables" the syncing and storing feeds from the external resource.
     */
    private static void registerExternalResource(ExternalFeedResource resource) {
        feedResources.put(resource.getResourceType(), resource);
    }

    private static AWSCredentialsProvider getAWSCredentials () {
        String S3_CREDENTIALS_FILENAME = DataManager.getConfigPropertyAsText("application.data.s3_credentials_file");
        if (S3_CREDENTIALS_FILENAME != null) {
            return new ProfileCredentialsProvider(S3_CREDENTIALS_FILENAME, "default");
        } else {
            // default credentials providers, e.g. IAM role
            return new DefaultAWSCredentialsProviderChain();
        }
    }
}
