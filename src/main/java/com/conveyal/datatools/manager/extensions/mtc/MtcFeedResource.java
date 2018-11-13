package com.conveyal.datatools.manager.extensions.mtc;

import com.amazonaws.services.s3.model.PutObjectRequest;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.extensions.ExternalFeedResource;
import com.conveyal.datatools.manager.models.ExternalFeedSourceProperty;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.FeedStore;
import com.conveyal.datatools.manager.persistence.Persistence;

import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.conveyal.datatools.manager.models.ExternalFeedSourceProperty.constructId;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;

/**
 * Created by demory on 3/30/16.
 */
public class MtcFeedResource implements ExternalFeedResource {

    private static final String rtdApi = DataManager.getConfigPropertyAsText("extensions.mtc.rtd_api");
    private static final String s3Bucket = DataManager.getConfigPropertyAsText("extensions.mtc.s3_bucket");
    private static final String s3Prefix = DataManager.getConfigPropertyAsText("extensions.mtc.s3_prefix");

    public static final String AGENCY_ID = "AgencyId";
    public static final String RESOURCE_TYPE = "MTC";
    public MtcFeedResource() { }

    @Override
    public String getResourceType() {
        return RESOURCE_TYPE;
    }

    @Override
    public String getBaseUrl() {
        return rtdApi + "/Carrier";
    }

    @Override
    public void importFeedsForProject(Project project, String authHeader) {
        LOG.info("Syncing with RTD");
        try {
            String json = getFeedsJson(getUrl(), authHeader);
            if (json == null) {
                LOG.warn("No JSON response found");
                return;
            }
            RtdCarrier[] results = mapper.readValue(json, RtdCarrier[].class);
            for (RtdCarrier car : results) {
                importFeed(project, car);
            }
        } catch(IOException e) {
            LOG.error("Could not read feeds from MTC RTD API");
            e.printStackTrace();
        }
    }

    /**
     * Do nothing for now. Creating a new agency for RTD requires adding the AgencyId property (when it was previously
     * null. See {@link #propertyUpdated(ExternalFeedSourceProperty, String, String)}.
     */
    @Override
    public void feedSourceCreated(FeedSource source, String authHeader) {
        LOG.info("Processing new FeedSource {} for RTD. (No action taken.)", source.name);
    }

    /**
     * Sync an updated property with the RTD database. Note: if the property is AgencyId and the value was previously
     * null create/register a new carrier with RTD.
     */
    @Override
    public void propertyUpdated(ExternalFeedSourceProperty property, String previousValue, String authHeader) {
        LOG.info("Update property in MTC carrier table: " + property.name);
        // Sync w/ RTD
        FeedSource source = Persistence.feedSources.getById(property.feedSourceId);
        // Get all props for feed source and resource type combination
        Map<String, String> props = new HashMap<>();
        Persistence.externalFeedSourceProperties
                .getFiltered(and(
                    eq("feedSourceId", source.id),
                    eq("resourceType", getResourceType())
                ))
                .forEach(p -> props.put(p.name, p.value));
        // Construct carrier from props map
        RtdCarrier carrier = mapper.convertValue(props, RtdCarrier.class);

        // Update external RTD database with new values.
        // Only create new carrier if a property exists for AgencyId (and there was no previous value)
        boolean createNew = property.name.equals(AGENCY_ID) && previousValue == null;
        writeCarrierToRtd(carrier, createNew, authHeader);
    }

    /**
     * When a new feed version for a feed source with an MTC reference is PUBLISHED (not necessarily just created)
     * this function should be called. Here we simply push the GTFS zipfile to the shared MTC S3 bucket for
     * processing/ingestion by the RTD (regional transit database).
     * @param feedVersion
     * @param authHeader
     */
    @Override
    public void feedVersionCreated(FeedVersion feedVersion, String authHeader) {

        if(s3Bucket == null) {
            LOG.error("Cannot push {} to S3 bucket. No bucket name specified.", feedVersion.id);
            return;
        }
        // Construct agency ID from feed source and retrieve from MongoDB.
        ExternalFeedSourceProperty agencyIdProp = Persistence.externalFeedSourceProperties.getById(
                constructId(feedVersion.parentFeedSource(), this.getResourceType(), AGENCY_ID)
        );

        if(agencyIdProp == null || agencyIdProp.value.equals("null")) {
            LOG.error("Could not read {} for FeedSource {}", AGENCY_ID, feedVersion.feedSourceId);
            return;
        }

        String keyName = String.format("%s%s.zip", s3Prefix, agencyIdProp.value);
        LOG.info("Pushing to MTC S3 Bucket: " + keyName);

        File file = feedVersion.retrieveGtfsFile();

        // upload GTFS to s3 bucket
        FeedStore.s3Client.putObject(new PutObjectRequest(s3Bucket, keyName, file));
    }

    /**
     * This method is used to write the carrier data to the RTD. A new carrier should only be created according to
     * the createNew boolean.
     */
    private void writeCarrierToRtd(RtdCarrier carrier, boolean createNew, String authHeader) {

        try {
            String carrierJson = mapper.writeValueAsString(carrier);

            URL rtdUrl = new URL(rtdApi + "/Carrier/" + (createNew ? "" : carrier.AgencyId));
            LOG.info("Writing to RTD URL: {}", rtdUrl);
            HttpURLConnection connection = (HttpURLConnection) rtdUrl.openConnection();

            connection.setRequestMethod(createNew ? "POST" : "PUT");
            connection.setDoOutput(true);
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setRequestProperty("Accept", "application/json");
            connection.setRequestProperty("Authorization", authHeader);

            OutputStreamWriter osw = new OutputStreamWriter(connection.getOutputStream());
            osw.write(carrierJson);
            osw.flush();
            osw.close();
            LOG.info("RTD API response: {}/{}", connection.getResponseCode(), connection.getResponseMessage());
        } catch (Exception e) {
            LOG.error("Error writing to RTD", e);
        }
    }
}
