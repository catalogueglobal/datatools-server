package com.conveyal.datatools.manager.extensions.mtc;

import com.amazonaws.services.s3.model.PutObjectRequest;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.extensions.ExternalFeedResource;
import com.conveyal.datatools.manager.models.ExternalFeedSourceProperty;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.FeedStore;

import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.lang.reflect.Field;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;

/**
 * Created by demory on 3/30/16.
 */
public class MtcFeedResource implements ExternalFeedResource {

    private static final String rtdApi = DataManager.getConfigPropertyAsText("extensions.mtc.rtd_api");
    private static final String s3Bucket = DataManager.getConfigPropertyAsText("extensions.mtc.s3_bucket");
    private static final String s3Prefix = DataManager.getConfigPropertyAsText("extensions.mtc.s3_prefix");
//    private static final String s3CredentialsFilename = ;

    public MtcFeedResource() { }

    @Override
    public String getResourceType() {
        return "MTC";
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

    @Override
    public void feedSourceCreated(FeedSource source, String authHeader) {
        LOG.info("Processing new FeedSource " + source.name + " for RTD");

        RtdCarrier carrier = new RtdCarrier();
        carrier.AgencyName = source.name;

        try {
            for (Field carrierField : carrier.getClass().getDeclaredFields()) {
                String fieldName = carrierField.getName();
                String fieldValue = carrierField.get(carrier) != null ? carrierField.get(carrier).toString() : null;
                ExternalFeedSourceProperty.updateOrCreate(source, this.getResourceType(), fieldName, fieldValue);
            }
        } catch (Exception e) {
            LOG.error("Error creating external properties for new FeedSource");
        }
    }

    @Override
    public void propertyUpdated(ExternalFeedSourceProperty property, String previousValue, String authHeader) {
        LOG.info("Update property in MTC carrier table: " + property.name);

        // sync w/ RTD
        String feedSourceId = property.getFeedSourceId();
        FeedSource source = FeedSource.get(feedSourceId);
        String resourceType = this.getResourceType();

        // get all props for feed source and resource type combination
        Map<String, String> props = ExternalFeedSourceProperty.findMultiple(source, resourceType);

        // construct carrier from props map
        RtdCarrier carrier = mapper.convertValue(props, RtdCarrier.class);

        // update external RTD database with new value
        // only create new carrier if a property exists for AgencyId (and there was no previous value)
        boolean createNew = property.name.equals("AgencyId") && previousValue == null;
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

        LOG.info("Pushing to MTC S3 Bucket " + s3Bucket);

        if(s3Bucket == null) return;

        ExternalFeedSourceProperty agencyIdProp =
                ExternalFeedSourceProperty.find(feedVersion.getFeedSource(), this.getResourceType(), "AgencyId");

        if(agencyIdProp == null || agencyIdProp.equals("null")) {
            LOG.error("Could not read AgencyId for FeedSource " + feedVersion.feedSourceId);
            return;
        }

        String keyName = this.s3Prefix + agencyIdProp.value + ".zip";
        LOG.info("Pushing to MTC S3 Bucket: " + keyName);

        File file = feedVersion.getGtfsFile();

        // upload GTFS to s3 bucket
        FeedStore.s3Client.putObject(new PutObjectRequest(s3Bucket, keyName, file));
    }

    /**
     * This method is used to write the carrier data to the RTD. A new carrier should only be created according to
     * the createNew boolean.
     * @param carrier
     * @param createNew
     * @param authHeader
     */
    private void writeCarrierToRtd(RtdCarrier carrier, boolean createNew, String authHeader) {

        try {
            String carrierJson = mapper.writeValueAsString(carrier);

            URL rtdUrl = new URL(rtdApi + "/Carrier/" + (createNew ? "" : carrier.AgencyId));
            LOG.info("Writing to RTD URL: " + rtdUrl);
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
            LOG.info("RTD API response: " + connection.getResponseCode() + " / " + connection.getResponseMessage());
        } catch (Exception e) {
            LOG.error("error writing to RTD");
            e.printStackTrace();
        }
    }
}
