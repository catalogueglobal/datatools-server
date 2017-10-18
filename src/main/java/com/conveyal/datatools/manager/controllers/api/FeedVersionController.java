package com.conveyal.datatools.manager.controllers.api;

import com.amazonaws.auth.policy.Statement;
import com.amazonaws.auth.policy.actions.S3Actions;
import com.conveyal.datatools.common.utils.SparkUtils;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.jobs.BuildTransportNetworkJob;
import com.conveyal.datatools.manager.jobs.CreateFeedVersionFromSnapshotJob;
import com.conveyal.datatools.manager.jobs.ProcessSingleFeedJob;
import com.conveyal.datatools.manager.jobs.ReadTransportNetworkJob;
import com.conveyal.datatools.manager.models.FeedDownloadToken;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.JsonViews;
import com.conveyal.datatools.manager.persistence.FeedStore;
import com.conveyal.datatools.manager.utils.HashUtils;
import com.conveyal.datatools.manager.utils.json.JsonManager;
import com.conveyal.r5.analyst.PointSet;
import com.conveyal.r5.analyst.cluster.AnalystClusterRequest;
import com.conveyal.r5.analyst.cluster.ResultEnvelope;
import com.conveyal.r5.analyst.cluster.TaskStatistics;
import com.conveyal.r5.api.util.LegMode;
import com.conveyal.r5.api.util.TransitModes;
import com.conveyal.r5.profile.ProfileRequest;
import com.conveyal.r5.profile.RepeatedRaptorProfileRouter;
import com.conveyal.r5.profile.StreetMode;
import com.conveyal.r5.streets.LinkedPointSet;
import com.conveyal.r5.transit.TransportNetwork;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import javax.servlet.MultipartConfigElement;
import javax.servlet.ServletException;
import javax.servlet.http.Part;

import static com.conveyal.datatools.common.utils.S3Utils.getS3Credentials;
import static com.conveyal.datatools.common.utils.SparkUtils.downloadFile;
import static com.conveyal.datatools.manager.controllers.api.FeedSourceController.requestFeedSource;
import static spark.Spark.*;

public class FeedVersionController  {
    enum Permission {
        VIEW, MANAGE
    }
    public static final Logger LOG = LoggerFactory.getLogger(FeedVersionController.class);
    private static ObjectMapper mapper = new ObjectMapper();
    public static JsonManager<FeedVersion> json =
            new JsonManager<FeedVersion>(FeedVersion.class, JsonViews.UserInterface.class);
    private static Set<String> networkBuildInProgress = new HashSet<>();
    private static Set<String> networkReadInProgress = new HashSet<>();
    private static Map<String, Long> networkCacheQueue = new HashMap();

    /**
     * Grab this feed version.
     * If you pass in ?summarized=true, don't include the full tree of validation results, only the counts.
     */
    public static FeedVersion getFeedVersion (Request req, Response res) throws JsonProcessingException {
        FeedVersion v = requestFeedVersion(req, "view");

        return v;
    }

    public static Collection<FeedVersion> getAllFeedVersions (Request req, Response res) throws JsonProcessingException {
        Auth0UserProfile userProfile = req.attribute("user");
        FeedSource s = requestFeedSourceById(req, "view");

        return s.getFeedVersions().stream()
                .collect(Collectors.toCollection(ArrayList::new));
    }
    private static FeedSource requestFeedSourceById(Request req, String action) {
        String id = req.queryParams("feedSourceId");
        if (id == null) {
            halt(SparkUtils.formatJSON("Please specify feedsourceId param", 400));
        }
        return requestFeedSource(req, FeedSource.get(id), action);
    }

    /**
     * Upload a feed version directly. This is done behind Backbone's back, and as such uses
     * x-multipart-formdata rather than a json blob. This is done because uploading files in a JSON
     * blob is not pretty, and we don't really need to get the Backbone object directly; page re-render isn't
     * a problem.
     *
     * Auto-fetched feeds are no longer restricted from having directly-uploaded versions, so we're not picky about
     * that anymore.
     * @return
     * @throws JsonProcessingException
     */
    public static Boolean createFeedVersion (Request req, Response res) throws IOException, ServletException {

        Auth0UserProfile userProfile = req.attribute("user");
        FeedSource s = requestFeedSourceById(req, "manage");

        FeedVersion latest = s.getLatest();
        FeedVersion v = new FeedVersion(s);
        v.setUser(userProfile);

        if (req.raw().getAttribute("org.eclipse.jetty.multipartConfig") == null) {
            MultipartConfigElement multipartConfigElement = new MultipartConfigElement(System.getProperty("java.io.tmpdir"));
            req.raw().setAttribute("org.eclipse.jetty.multipartConfig", multipartConfigElement);
        }

        Part part = req.raw().getPart("file");
        LOG.info("Saving feed from upload {}", s);


        InputStream uploadStream;
        File file = null;
        try {
            uploadStream = part.getInputStream();

            /**
             * Set last modified based on value of query param. This is determined/supplied by the client
             * request because this data gets lost in the uploadStream otherwise.
             */
            file = v.newGtfsFile(uploadStream, Long.valueOf(req.queryParams("lastModified")));
            LOG.info("Last modified: {}", new Date(file.lastModified()));
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("Unable to open input stream from upload");
            halt(400, "Unable to read uploaded feed");
        }

        v.hash();
        // TODO: fix hash() call when called in this context.  Nothing gets hashed because the file has not been saved yet.
        v.hash = HashUtils.hashFile(file);

        // Check that hashes don't match (as long as v and latest are not the same entry)
        if (latest != null && latest.hash.equals(v.hash)) {
            LOG.error("Upload version {} matches latest version {}.", v.id, latest.id);
            File gtfs = v.getGtfsFile();
            if (gtfs != null) {
                gtfs.delete();
            } else {
                file.delete();
                LOG.warn("File deleted");
            }
            // Uploaded feed is same as latest version
            v.delete();
            halt(304);
        }

        v.name = v.getFormattedTimestamp() + " Upload";
//        v.fileTimestamp
        v.userId = userProfile.getUser_id();
        v.save();

        // must be handled by executor because it
        ProcessSingleFeedJob processSingleFeedJob = new ProcessSingleFeedJob(v, userProfile.getUser_id());
        DataManager.heavyExecutor.execute(processSingleFeedJob);

        return true;
    }

    public static boolean createFeedVersionFromSnapshot (Request req, Response res) throws IOException, ServletException {

        Auth0UserProfile userProfile = req.attribute("user");
        // TODO: should this be edit privilege?
        FeedSource s = requestFeedSourceById(req, "manage");
        FeedVersion v = new FeedVersion(s);
        CreateFeedVersionFromSnapshotJob createFromSnapshotJob =
                new CreateFeedVersionFromSnapshotJob(v, req.queryParams("snapshotId"), userProfile.getUser_id());
        createFromSnapshotJob.addNextJob(new ProcessSingleFeedJob(v, userProfile.getUser_id()));
        DataManager.heavyExecutor.execute(createFromSnapshotJob);

        return true;
    }

    public static FeedVersion deleteFeedVersion(Request req, Response res) {
        FeedVersion version = requestFeedVersion(req, "manage");

        version.delete();

        // renumber the versions
        Collection<FeedVersion> versions = version.getFeedSource().getFeedVersions();
        FeedVersion[] versionArray = versions.toArray(new FeedVersion[versions.size()]);
        Arrays.sort(versionArray, (v1, v2) -> v1.updated.compareTo(v2.updated));
        for(int i = 0; i < versionArray.length; i++) {
            FeedVersion v = versionArray[i];
            v.version = i + 1;
            v.save();
        }

        return version;
    }

    public static FeedVersion requestFeedVersion(Request req, String action) {
        String id = req.params("id");

        FeedVersion version = FeedVersion.get(id);
        if (version == null) {
            halt(404, "Version ID does not exist");
        }
        // performs permissions checks for at feed source level and halts if any issues
        requestFeedSource(req, version.getFeedSource(), action);
        return version;
    }

    public static JsonNode getValidationResult(Request req, Response res) {
        return getValidationResult(req, res, false);
    }

    public static JsonNode getPublicValidationResult(Request req, Response res) {
        return getValidationResult(req, res, true);
    }

    public static JsonNode getValidationResult(Request req, Response res, boolean checkPublic) {
        FeedVersion version = requestFeedVersion(req, "view");

        return version.getValidationResult(false);
    }

    public static JsonNode getIsochrones(Request req, Response res) {
        if (!DataManager.isModuleEnabled("validator")) {
            halt(400, SparkUtils.formatJSON("Isochrone generation not enabled in this application."));
        }

        Auth0UserProfile userProfile = req.attribute("user");
        FeedVersion version = requestFeedVersion(req, "view");
        TransportNetwork transportNetwork = buildOrReadTransportNetwork(version, userProfile);
        if (transportNetwork != null) {
            // remove version from list of reading network
            if (networkBuildInProgress.contains(version.id)) {
                networkBuildInProgress.remove(version.id);
            }
            AnalystClusterRequest clusterRequest = buildProfileRequest(req);
            return getRouterResult(transportNetwork, clusterRequest);
        } else {
            halt(202, SparkUtils.formatJSON("Reading transport network, please try again later."));
        }
        return null;
    }

    public static TransportNetwork buildOrReadTransportNetwork(FeedVersion version, Auth0UserProfile userProfile) {
            if (DataManager.transportNetworkCache.containsTransportNetwork(version.id)) {
                // Simplest case. Network has already been loaded in cache, so we just return it.
                try {
                    if (networkReadInProgress.contains(version.id)) {
                        networkReadInProgress.remove(version.id);
                    }
                    return DataManager.transportNetworkCache.getTransportNetwork(version.id);
                } catch (Exception e) {
                    LOG.error("Unknown error accessing transport network.", e);
                    halt(400, SparkUtils.formatJSON("Unknown error accessing transport network."));
                }
            } else {
                // TransportNetwork has not been read before and a cache load has not been triggered.
                // TODO: add logic that keeps transport networks from being evicted while being used (below code checks load time).
//                // This check is performed because the loading cache does not evict items even if they have expired.
//                // This is explained here: https://github.com/google/guava/wiki/CachesExplained#when-does-cleanup-happen
//                String waitMessage = "Sorry, isochrones for this feed are not available at the moment. Please wait approximately ";
//                long cacheDurationInMillis = DataManager.transportNetworkCache.timeUnit.toMillis(
//                        DataManager.transportNetworkCache.duration);
//                long timeSinceEarliestLoad = DataManager.transportNetworkCache.getTimeSinceEarliestLoad();
//                if (DataManager.transportNetworkCache.isAtCapacity() &&
//                        timeSinceEarliestLoad < cacheDurationInMillis) {
//                    // Only tell the requester that isochrones are unavailable if the cache is at capacity and
//                    // no cached network has "expired" yet.
//                    if (networkCacheQueue.containsKey(version.id)) {
//                        long firstRequestTimestamp = networkCacheQueue.get(version.id);
//                        long timeSinceFirstRequest = System.currentTimeMillis() - firstRequestTimestamp;
//                        if (timeSinceFirstRequest > cacheDurationInMillis) {
//                            // If 10 minutes has passed, remove from queue and pass through to begin loading the version
//                            // into the cache.
//                            networkCacheQueue.remove(version.id);
//                        } else {
//                            String waitTime = String.join(" ",
//                                    String.valueOf(((double)cacheDurationInMillis - timeSinceFirstRequest) / 1000 / 60),
//                                    DataManager.transportNetworkCache.timeUnit.toString());
//                            halt(202, SparkUtils.formatJSON(waitMessage + waitTime, 202));
//                        }
//                    } else {
//                        // Put feed version in the queue
//                        String waitTime = String.join(" ",
//                                String.valueOf(timeSinceEarliestLoad),
//                                DataManager.transportNetworkCache.timeUnit.toString());
//                        networkCacheQueue.put(version.id, System.currentTimeMillis());
//                        halt(202, SparkUtils.formatJSON(waitMessage + waitTime, 202));
//                    }
//
//                }

                // Here we trigger an inputStream read on transport network file to determine if it exists
                // (i.e., a network has already been built) and throw an exception if not.
                if (version.getTransportNetworkPath().exists()) {
                    if (networkBuildInProgress.contains(version.id)) {
                        // A transport network exists, but it was just built. Remove the version from the
                        // loads in progress list, and return the network.
                        try {
                            // If we get to this point, a network was recently built after an API request.
                            TransportNetwork tn = DataManager.transportNetworkCache.getTransportNetwork(version.id);
                            networkBuildInProgress.remove(version.id);
                            return tn;
                        } catch (Exception e) {
                            LOG.error("Could not read transport network.", e);
                            halt(400, SparkUtils.formatJSON("Could not read transport network."));
                        }
                    } else {
                        // A transport network exists, but has not been read yet. Call read network job or wait for read
                        // to finish.
                        if (!networkReadInProgress.contains(version.id)) {
                            networkReadInProgress.add(version.id);
                            ReadTransportNetworkJob readTransportNetworkJob =
                                    new ReadTransportNetworkJob(version, userProfile.getUser_id());

                            DataManager.heavyExecutor.execute(readTransportNetworkJob);
                        }
                        // Notify user that read is in progress.
                        halt(202, SparkUtils.formatJSON("Try again later. Reading transport network", 202));
                    }
                } else {
                    // If transport network has not been built yet (i.e., file does not exist), add to builds in
                    // progress list, and begin build job.
                    if (!networkBuildInProgress.contains(version.id)) {
                        LOG.warn("Transport network not found. Beginning build.");
                        networkBuildInProgress.add(version.id);
                        BuildTransportNetworkJob buildTransportNetworkJob =
                                new BuildTransportNetworkJob(version, userProfile.getUser_id());
                        ReadTransportNetworkJob readTransportNetworkJob =
                                new ReadTransportNetworkJob(version, userProfile.getUser_id());
                        buildTransportNetworkJob.addNextJob(readTransportNetworkJob);
                        DataManager.heavyExecutor.execute(buildTransportNetworkJob);
                    }
                    // Notify user that build is in progress.
                    halt(202, SparkUtils.formatJSON("Try again later. Building transport network", 202));
                }
            }
        return null;
    }

    private static JsonNode getRouterResult(TransportNetwork transportNetwork, AnalystClusterRequest clusterRequest) {
        PointSet targets;
        if (transportNetwork.gridPointSet == null) {
            transportNetwork.rebuildLinkedGridPointSet();
        }
        targets = transportNetwork.gridPointSet;
        StreetMode mode = StreetMode.WALK;
        final LinkedPointSet linkedTargets = targets.link(transportNetwork.streetLayer, mode);
        RepeatedRaptorProfileRouter router =
                new RepeatedRaptorProfileRouter(transportNetwork, clusterRequest, linkedTargets, new TaskStatistics());
        ResultEnvelope result = router.route();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            JsonGenerator jsonGenerator = new JsonFactory().createGenerator(out);
            jsonGenerator.writeStartObject();
            result.avgCase.writeIsochrones(jsonGenerator);
            jsonGenerator.writeEndObject();
            jsonGenerator.close();
            out.close();
            String outString = new String( out.toByteArray(), StandardCharsets.UTF_8 );
            return mapper.readTree(outString);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static AnalystClusterRequest buildProfileRequest(Request req) {
        // required fields?
        Double fromLat = Double.valueOf(req.queryParams("fromLat"));
        Double fromLon = Double.valueOf(req.queryParams("fromLon"));
        Double toLat = Double.valueOf(req.queryParams("toLat"));
        Double toLon = Double.valueOf(req.queryParams("toLon"));
        LocalDate date = req.queryParams("date") != null
                ? LocalDate.parse(req.queryParams("date"), DateTimeFormatter.ISO_LOCAL_DATE)
                : LocalDate.now(); // 2011-12-03

        // optional with defaults
        Integer fromTime = req.queryParams("fromTime") != null ? Integer.valueOf(req.queryParams("fromTime")) : 9 * 3600;
        Integer toTime = req.queryParams("toTime") != null ? Integer.valueOf(req.queryParams("toTime")) : 10 * 3600;

        // build request with transit as default mode
        AnalystClusterRequest clusterRequest = new AnalystClusterRequest();
        clusterRequest.profileRequest = new ProfileRequest();
        clusterRequest.profileRequest.transitModes = EnumSet.of(TransitModes.TRANSIT);
        clusterRequest.profileRequest.accessModes = EnumSet.of(LegMode.WALK);
        clusterRequest.profileRequest.date = date;
        clusterRequest.profileRequest.fromLat = fromLat;
        clusterRequest.profileRequest.fromLon = fromLon;
        clusterRequest.profileRequest.toLat = toLat;
        clusterRequest.profileRequest.toLon = toLon;
        clusterRequest.profileRequest.fromTime = fromTime;
        clusterRequest.profileRequest.toTime = toTime;
        clusterRequest.profileRequest.egressModes = EnumSet.of(LegMode.WALK);
        clusterRequest.profileRequest.zoneId = ZoneId.of("America/New_York");

        return clusterRequest;
    }

    public static Boolean renameFeedVersion (Request req, Response res) throws JsonProcessingException {
        FeedVersion v = requestFeedVersion(req, "manage");

        String name = req.queryParams("name");
        if (name == null) {
            halt(400, "Name parameter not specified");
        }

        v.name = name;
        v.save();
        return true;
    }

    private static Object downloadFeedVersionDirectly(Request req, Response res) {
        FeedVersion version = requestFeedVersion(req, "view");
        return downloadFile(version.getGtfsFile(), version.id, res);
    }

    /**
     * Returns credentials that a client may use to then download a feed version. Functionality
     * changes depending on whether application.data.use_s3_storage config property is true.
     */
    public static Object getFeedDownloadCredentials(Request req, Response res) {
        FeedVersion version = requestFeedVersion(req, "view");

        // if storing feeds on s3, return temporary s3 credentials for that zip file
        if (DataManager.useS3) {
            return getS3Credentials(DataManager.awsRole, DataManager.feedBucket, FeedStore.s3Prefix + version.id, Statement.Effect.Allow, S3Actions.GetObject, 900);
        } else {
            // when feeds are stored locally, single-use download token will still be used
            FeedDownloadToken token = new FeedDownloadToken(version);
            token.save();
            return token;
        }
    }

    private static JsonNode validate (Request req, Response res) {
        FeedVersion version = requestFeedVersion(req, "manage");
        return version.getValidationResult(true);
    }

    private static FeedVersion publishToExternalResource (Request req, Response res) {
        FeedVersion version = requestFeedVersion(req, "manage");

        // notify any extensions of the change
        for(String resourceType : DataManager.feedResources.keySet()) {
            DataManager.feedResources.get(resourceType).feedVersionCreated(version, null);
        }
        FeedSource fs = version.getFeedSource();
        fs.publishedVersionId = version.id;
        fs.save();
        return version;
    }

    private static Object downloadFeedVersionWithToken (Request req, Response res) {
        FeedDownloadToken token = FeedDownloadToken.get(req.params("token"));

        if(token == null || !token.isValid()) {
            halt(400, "Feed download token not valid");
        }

        FeedVersion version = token.getFeedVersion();

        token.delete();

        return downloadFile(version.getGtfsFile(), version.id, res);
    }

    public static void register (String apiPrefix) {
        get(apiPrefix + "secure/feedversion/:id", FeedVersionController::getFeedVersion, json::write);
        get(apiPrefix + "secure/feedversion/:id/download", FeedVersionController::downloadFeedVersionDirectly);
        get(apiPrefix + "secure/feedversion/:id/downloadtoken", FeedVersionController::getFeedDownloadCredentials, json::write);
        get(apiPrefix + "secure/feedversion/:id/validation", FeedVersionController::getValidationResult, json::write);
        post(apiPrefix + "secure/feedversion/:id/validate", FeedVersionController::validate, json::write);
        get(apiPrefix + "secure/feedversion/:id/isochrones", FeedVersionController::getIsochrones, json::write);
        get(apiPrefix + "secure/feedversion", FeedVersionController::getAllFeedVersions, json::write);
        post(apiPrefix + "secure/feedversion", FeedVersionController::createFeedVersion, json::write);
        post(apiPrefix + "secure/feedversion/fromsnapshot", FeedVersionController::createFeedVersionFromSnapshot, json::write);
        put(apiPrefix + "secure/feedversion/:id/rename", FeedVersionController::renameFeedVersion, json::write);
        post(apiPrefix + "secure/feedversion/:id/publish", FeedVersionController::publishToExternalResource, json::write);
        delete(apiPrefix + "secure/feedversion/:id", FeedVersionController::deleteFeedVersion, json::write);

        get(apiPrefix + "public/feedversion", FeedVersionController::getAllFeedVersions, json::write);
        get(apiPrefix + "public/feedversion/:id/validation", FeedVersionController::getPublicValidationResult, json::write);
        get(apiPrefix + "public/feedversion/:id/downloadtoken", FeedVersionController::getFeedDownloadCredentials, json::write);

        get(apiPrefix + "downloadfeed/:token", FeedVersionController::downloadFeedVersionWithToken);

    }
}
