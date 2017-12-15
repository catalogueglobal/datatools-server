package com.conveyal.datatools.manager.controllers.api;

import com.conveyal.datatools.common.utils.Consts;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.persistence.FeedStore;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.utils.json.JsonUtil;
import com.conveyal.gtfs.loader.Feed;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.io.ByteStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

import static com.conveyal.datatools.common.utils.SparkUtils.haltWithError;
import static com.conveyal.datatools.manager.controllers.api.FeedVersionController.writeUploadedFileFromRequest;
import static spark.Spark.get;
import static spark.Spark.post;

/**
 * Created by demory on 4/13/16.
 */
public class GtfsPlusController {

    public static final Logger LOG = LoggerFactory.getLogger(GtfsPlusController.class);
    // FIXME: Fix use of FeedStore
    private static FeedStore gtfsPlusStore;


    private static boolean uploadGtfsPlusFile (Request req, Response res) throws IOException, ServletException {
        // FIXME: Missing permissions check?
        //FeedSource s = FeedSource.retrieveById(req.queryParams("feedSourceId"));
        String feedVersionId = req.params("versionid");
        LOG.info("Saving GTFS+ feed {} from upload for version " + feedVersionId);
        File newGtfsPlusFile = gtfsPlusStore.getFileForId(feedVersionId);
        writeUploadedFileFromRequest(req, newGtfsPlusFile);
        // FIXME: Update return method to JSON formatted response.
        return true;
    }

    private static HttpServletResponse getGtfsPlusFile(Request req, Response res) {
        String feedVersionId = req.params("versionid");
        LOG.info("Downloading GTFS+ file for FeedVersion " + feedVersionId);

        // check for saved
        File file = gtfsPlusStore.getFeed(feedVersionId);
        if(file == null) {
            return getGtfsPlusFromGtfs(feedVersionId, res);
        }
        LOG.info("Returning updated GTFS+ data");
        return downloadGtfsPlusFile(file, res);
    }

    private static HttpServletResponse getGtfsPlusFromGtfs(String feedVersionId, Response res) {
        LOG.info("Extracting GTFS+ data from main GTFS feed");
        FeedVersion version = Persistence.feedVersions.getById(feedVersionId);
        File gtfsPlusFile = null;
        try {

            // create a new zip file to only contain the GTFS+ tables
            gtfsPlusFile = gtfsPlusStore.getFileForId(feedVersionId + "_gtfsplus");
            // Delete on exit because this is essentially scratch data.
            gtfsPlusFile.deleteOnExit();
            ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(gtfsPlusFile));
            // iterate through the existing GTFS file, copying any GTFS+ tables
            copyEntries(version.retrieveGtfsFile(), zos, true);
            // Close zip output stream after operation is complete.
            zos.close();
        } catch (Exception e) {
            LOG.error("Error extracting GTFS+ files for " + feedVersionId, e);
            haltWithError(500, "Error getting GTFS+ file from GTFS", e);
        }

        return downloadGtfsPlusFile(gtfsPlusFile, res);
    }

    /**
     * Copies entries from original GTFS file to zip output stream. Depending on boolean parameter, function will copy
     * only standard GTFS files or only GTFS+ files. This function does not close the zip output stream after copying
     * so that additional copy operations can be performed.
     */
    private static void copyEntries(File originalGtfsFile, ZipOutputStream zos, boolean copyPlusFiles) throws IOException {
        ZipFile originalZipFile = new ZipFile(originalGtfsFile);
        // Create set of GTFS+ table filenames.
        Set<String> gtfsPlusTables = new HashSet<>();
        DataManager.gtfsPlusConfig.forEach(table -> gtfsPlusTables.add(table.get("name").asText()));
        final Enumeration<? extends ZipEntry> entries = originalZipFile.entries();
        while (entries.hasMoreElements()) {
            final ZipEntry entry = entries.nextElement();
            String fileName = entry.getName();
            boolean isPlusFile = gtfsPlusTables.contains(fileName);
            boolean isNonStandard = isPlusFile || fileName.startsWith("_");
            if(!copyPlusFiles && isNonStandard) {
                // Skip GTFS+ and non-standard tables if copying only GTFS files.
                continue;
            }
            if(copyPlusFiles && !isPlusFile) {
                // Skip non-GTFS+ files if copying only GTFS+ files.
                continue;
            }
            // Create a new empty ZipEntry and copy the contents.
            copyZipEntry(originalZipFile, entry, zos);
        }
    }

    private static HttpServletResponse downloadGtfsPlusFile(File file, Response res) {
        res.raw().setContentType("application/octet-stream");
        res.raw().setHeader("Content-Disposition", "attachment; filename=" + file.getName() + ".zip");

        try {
            BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(res.raw().getOutputStream());
            BufferedInputStream bufferedInputStream = new BufferedInputStream(new FileInputStream(file));

            byte[] buffer = new byte[1024];
            int len;
            while ((len = bufferedInputStream.read(buffer)) > 0) {
                bufferedOutputStream.write(buffer, 0, len);
            }

            bufferedOutputStream.flush();
            bufferedOutputStream.close();
        } catch (Exception e) {
            haltWithError(500, "Error serving GTFS+ file", e);
        }

        return res.raw();
    }

    private static Long getGtfsPlusFileTimestamp(Request req, Response res) {
        String feedVersionId = req.params("versionid");

        // check for saved GTFS+ data
        File file = gtfsPlusStore.getFeed(feedVersionId);
        if (file == null) {
            FeedVersion feedVersion = Persistence.feedVersions.getById(feedVersionId);
            if (feedVersion != null) {
                file = feedVersion.retrieveGtfsFile();
            } else {
                haltWithError(400, "Feed version ID is not valid");
            }
        }

        if (file != null) {
            return file.lastModified();
        } else {
            haltWithError(400, "Feed version file not found");
            return null;
        }
    }

    private static Boolean publishGtfsPlusFile(Request req, Response res) {
        Auth0UserProfile profile = req.attribute("user");
        String feedVersionId = req.params("versionid");
        LOG.info("Publishing GTFS+ for " + feedVersionId);
        File plusFile = gtfsPlusStore.getFeed(feedVersionId);
        if(plusFile == null || !plusFile.exists()) {
            haltWithError(400, "No saved GTFS+ data for version");
        }

        FeedVersion originalFeedVersion = Persistence.feedVersions.getById(feedVersionId);

        // Create new feed version for newly published GTFS+ file.
        FeedVersion newFeedVersion = new FeedVersion(originalFeedVersion.parentFeedSource());
        File newGtfsFile = DataManager.feedStore.getFileForId(newFeedVersion.id);

        File originalGtfsFile = originalFeedVersion.retrieveGtfsFile();
        // FIXME: Turn merge operation into MonitorableJob?
        boolean success = mergeGtfsPlusTables(originalGtfsFile, plusFile, newGtfsFile);
        if (success) {
            newFeedVersion.updateFields(newFeedVersion.formattedTimestamp() + " GTFS+ update", newGtfsFile, FeedSource.FeedRetrievalMethod.PRODUCED_IN_HOUSE);
            // validation for the main GTFS content hasn't changed
            // FIXME: What about namespace. Does that need to change? Perhaps not because GTFS+ are not stored in database.
            // Maybe we should be "cloning" the originalFeedVersion and just changing its user/ID/name/GTFS file/retrieval method.
            newFeedVersion.feedLoadResult = originalFeedVersion.feedLoadResult;
            newFeedVersion.validationResult = originalFeedVersion.validationResult;
            newFeedVersion.storeUser(profile);
            Persistence.feedVersions.create(newFeedVersion);
        } else {
            haltWithError(400, "Unknown error merging GTFS+ data with GTFS");
        }
        return true;
    }

    private static boolean mergeGtfsPlusTables(File originalFile, File gtfsPlusFile, File outputGtfsFile) {
        try {
            // Create a new zip file that contains the original standard GTFS files with the GTFS+ tables merged into
            // the file.
            ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(outputGtfsFile));
            // iterate through the original GTFS file, copying all standard (non-GTFS+) tables into new GTFS file
            copyEntries(originalFile, zos, false);
            // Iterate over the GTFS+ file, copying all entries into output file
            copyEntries(gtfsPlusFile, zos, true);
            // Close zip output stream finally.
            zos.close();
            return true;
        } catch (Exception e) {
            LOG.error("Error merging new plus files into GTFS", e);
            return false;
        }
    }

    /**
     * Handles copying an existing zip entry into a new zip file (zip output stream).
     */
    private static void copyZipEntry(ZipFile originalZipFile, ZipEntry originalEntry, ZipOutputStream zipOutputStream) throws IOException {
        // Create new entry using fileName of original entry.
        ZipEntry newEntry = new ZipEntry(originalEntry.getName());
        zipOutputStream.putNextEntry(newEntry);
        InputStream in = originalZipFile.getInputStream(originalEntry);
        ByteStreams.copy(in, zipOutputStream);
        in.close();
        zipOutputStream.closeEntry();
    }

    private static Collection<ValidationIssue> getGtfsPlusValidation(Request req, Response res) {
        String feedVersionId = req.params("versionid");
        LOG.info("Validating GTFS+ for " + feedVersionId);
        FeedVersion feedVersion = Persistence.feedVersions.getById(feedVersionId);

        List<ValidationIssue> issues = new LinkedList<>();


        // load the main GTFS
        Feed feed = feedVersion.retrieveFeed();
        // check for saved GTFS+ data
        File file = gtfsPlusStore.getFeed(feedVersionId);
        if (file == null) {
            LOG.warn("GTFS+ file not found, loading from main version GTFS.");
            file = feedVersion.retrieveGtfsFile();
        }
        int gtfsPlusTableCount = 0;
        try {
            ZipFile zipFile = new ZipFile(file);
            final Enumeration<? extends ZipEntry> entries = zipFile.entries();
            while (entries.hasMoreElements()) {
                final ZipEntry entry = entries.nextElement();
                for(int i = 0; i < DataManager.gtfsPlusConfig.size(); i++) {
                    JsonNode tableNode = DataManager.gtfsPlusConfig.get(i);
                    if(tableNode.get("name").asText().equals(entry.getName())) {
                        LOG.info("Validating GTFS+ table: " + entry.getName());
                        gtfsPlusTableCount++;
                        validateTable(issues, tableNode, zipFile.getInputStream(entry), feed);
                    }
                }
            }

        } catch(Exception e) {
            LOG.error("Could not validate GTFS+ data for " + feedVersionId, e);
            haltWithError(400, "Could not validate GTFS+ data.", e);
        }
        LOG.info("GTFS+ tables found: {}/{}", gtfsPlusTableCount, DataManager.gtfsPlusConfig.size());
        return issues;
    }

    /**
     * Validate a GTFS+ table.
     */
    private static void validateTable(Collection<ValidationIssue> issues, JsonNode tableNode, InputStream inputStream, Feed gtfsFeed) throws IOException {

        String tableId = tableNode.get("id").asText();
        BufferedReader in = new BufferedReader(new InputStreamReader(inputStream));
        String line = in.readLine();
        String[] fields = line.split(",");
        List<String> fieldList = Arrays.asList(fields);
        for (String field : fieldList) {
            field = field.toLowerCase();
        }

        JsonNode[] fieldNodes = new JsonNode[fields.length];

        JsonNode fieldsNode = tableNode.get("fields");
        for(int i = 0; i < fieldsNode.size(); i++) {
            JsonNode fieldNode = fieldsNode.get(i);
            int index = fieldList.indexOf(fieldNode.get("name").asText());
            if(index != -1) fieldNodes[index] = fieldNode;
        }

        int rowIndex = 0;
        while((line = in.readLine()) != null) {
            String[] values = line.split(Consts.COLUMN_SPLIT, -1);
            for(int v=0; v < values.length; v++) {
                validateTableValue(issues, tableId, rowIndex, values[v], fieldNodes[v], gtfsFeed);
            }
            rowIndex++;
        }
    }

    private static void validateTableValue(Collection<ValidationIssue> issues, String tableId, int rowIndex, String value, JsonNode fieldNode, Feed gtfsFeed) {
        if(fieldNode == null) return;
        String fieldName = fieldNode.get("name").asText();

        if(fieldNode.get("required") != null && fieldNode.get("required").asBoolean()) {
            if(value == null || value.length() == 0) {
                issues.add(new ValidationIssue(tableId, fieldName, rowIndex, "Required field missing value"));
            }
        }

        switch(fieldNode.get("inputType").asText()) {
            case "DROPDOWN":
                boolean invalid = true;
                ArrayNode options = (ArrayNode) fieldNode.get("options");
                for (JsonNode option : options) {
                    String optionValue = option.get("value").asText();

                    // NOTE: per client's request, this check has been made case insensitive
                    boolean valuesAreEqual = optionValue.equalsIgnoreCase(value);

                    // if value is found in list of options, break out of loop
                    if (valuesAreEqual || (!fieldNode.get("required").asBoolean() && value.equals(""))) {
                        invalid = false;
                        break;
                    }
                }
                if (invalid) {
                    issues.add(new ValidationIssue(tableId, fieldName, rowIndex, "Value: " + value + " is not a valid option."));
                }
                break;
            case "TEXT":
                // check if value exceeds max length requirement
                if(fieldNode.has("maxLength")) {
                    int maxLength = fieldNode.get("maxLength").asInt();
                    if (value != null && value.length() > maxLength) {
                        issues.add(new ValidationIssue(tableId, fieldName, rowIndex, "Text value exceeds the max. length of "+maxLength));
                    }
                }
                break;
            case "GTFS_ROUTE":
                // FIXME: fix gtfs+ loading/validating for sql-load. The get call below compiles, but might not actually
                // be the way to check referential integrity here.
                if(gtfsFeed.routes.get(value) == null) {
                    issues.add(new ValidationIssue(tableId, fieldName, rowIndex, "Route ID "+ value + " not found in GTFS"));
                }
                break;
            case "GTFS_STOP":
                // FIXME: fix gtfs+ loading/validating for sql-load. The get call below compiles, but might not actually
                // be the way to check referential integrity here.
                if(gtfsFeed.stops.get(value) == null) {
                    issues.add(new ValidationIssue(tableId, fieldName, rowIndex, "Stop ID "+ value + " not found in GTFS"));
                }
                break;
            case "GTFS_TRIP":
                // FIXME: fix gtfs+ loading/validating for sql-load. The get call below compiles, but might not actually
                // be the way to check referential integrity here.
                if(gtfsFeed.trips.get(value) == null) {
                    issues.add(new ValidationIssue(tableId, fieldName, rowIndex, "Trip ID "+ value + " not found in GTFS"));
                }
                break;
            case "GTFS_FARE":
                // FIXME: fix gtfs+ loading/validating for sql-load. The get call below compiles, but might not actually
                // be the way to check referential integrity here.
//                if(gtfsFeed.fares.get(value) == null) {
//                    issues.add(new ValidationIssue(tableId, fieldName, rowIndex, "Fare ID "+ value + " not found in GTFS"));
//                }
                break;
            case "GTFS_SERVICE":
                // FIXME: fix gtfs+ loading/validating for sql-load. The get call below compiles, but might not actually
                // be the way to check referential integrity here.
                if (gtfsFeed.calendars.get(value) == null) {
                    issues.add(new ValidationIssue(tableId, fieldName, rowIndex, "Service ID "+ value + " not found in GTFS"));
                }
                break;
        }

    }

    public static class ValidationIssue implements Serializable {
        private static final long serialVersionUID = 1L;
        public String tableId;
        public String fieldName;
        public int rowIndex;
        public String description;

        public ValidationIssue(String tableId, String fieldName, int rowIndex, String description) {
            this.tableId = tableId;
            this.fieldName = fieldName;
            this.rowIndex = rowIndex;
            this.description = description;
        }
    }

    public static void register(String apiPrefix, FeedStore feedStore) {
        gtfsPlusStore = feedStore;
        post(apiPrefix + "secure/gtfsplus/:versionid", GtfsPlusController::uploadGtfsPlusFile, JsonUtil.objectMapper::writeValueAsString);
        get(apiPrefix + "secure/gtfsplus/:versionid", GtfsPlusController::getGtfsPlusFile);
        get(apiPrefix + "secure/gtfsplus/:versionid/timestamp", GtfsPlusController::getGtfsPlusFileTimestamp, JsonUtil.objectMapper::writeValueAsString);
        get(apiPrefix + "secure/gtfsplus/:versionid/validation", GtfsPlusController::getGtfsPlusValidation, JsonUtil.objectMapper::writeValueAsString);
        post(apiPrefix + "secure/gtfsplus/:versionid/publish", GtfsPlusController::publishGtfsPlusFile, JsonUtil.objectMapper::writeValueAsString);
    }
}
