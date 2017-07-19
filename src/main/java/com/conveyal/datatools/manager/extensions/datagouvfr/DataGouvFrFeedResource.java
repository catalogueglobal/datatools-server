package com.conveyal.datatools.manager.extensions.datagouvfr;

import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.extensions.ExternalFeedResource;
import com.conveyal.datatools.manager.models.ExternalFeedSourceProperty;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.Project;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * Created by demory on 3/31/16.
 */

public class DataGouvFrFeedResource implements ExternalFeedResource {

    public static final Logger LOG = LoggerFactory.getLogger(DataGouvFrFeedResource.class);

    private String api;

    public DataGouvFrFeedResource() {
        api = DataManager.getConfigPropertyAsText("extensions.datagouvfr.api");
    }

    @Override
    public String getResourceType() {
        return "DATAGOUVFR";
    }

    @Override
    public void importFeedsForProject(Project project, String authHeader) {
        LOG.info("Importing DataGouvFr feeds");
        URL url = null;
        ObjectMapper mapper = new ObjectMapper();
        String locationFilter = "";
        String next_page = api;
        LOG.info("Starting loop");

        do {
            try {
                LOG.info("Constructing url: {}", next_page);
                url = new URL(next_page);
            } catch (MalformedURLException ex) {
                LOG.error("Error requesting datagouv API URL: {}", next_page);
            }

            try {
                HttpURLConnection con = (HttpURLConnection) url.openConnection();

                // optional default is GET
                con.setRequestMethod("GET");

                int responseCode = con.getResponseCode();
                System.out.println("\nSending 'GET' request to URL : " + url);
                System.out.println("Response Code : " + responseCode);

                BufferedReader in = new BufferedReader(
                        new InputStreamReader(con.getInputStream()));
                String inputLine;
                StringBuffer response = new StringBuffer();

                while ((inputLine = in.readLine()) != null) {
                    response.append(inputLine);
                }
                in.close();

                String json = response.toString();
                JsonNode node = mapper.readTree(json);
                next_page = node.get("next_page").asText();
                LOG.info("New next_page: {}", next_page);
                for (JsonNode dataset : node.get("data")) {
                    DataGouvFrFeed dgfFeed = new DataGouvFrFeed(dataset);
                    LOG.info("Format: {}", dgfFeed.feed_format);
                    if (!dgfFeed.feed_format.contentEquals("gtfs") &&
                        !dgfFeed.feed_format.contentEquals("csv")) {
                        continue;
                    }

                    FeedSource source = null;

                    // check if a feed already exists with this id
                    for (FeedSource existingSource : project.getProjectFeedSources()) {
                        ExternalFeedSourceProperty dgfIdProp =
                                ExternalFeedSourceProperty.find(existingSource,
                                        this.getResourceType(), "dgf_id")
                        ;
                        if (dgfIdProp != null && dgfIdProp.value.equals(dgfFeed.dgf_id)) {
                            source = existingSource;
                        }
                    }

                    String feedId = dgfFeed.dgf_id;

                    if (source == null) {
                        source = new FeedSource(feedId);
                        LOG.info("Creating new feed source: {}", feedId);
                    }
                    else {
                        source.name = dgfFeed.title;
                        LOG.info("Syncing properties: {}", feedId);
                    }
                    LOG.info("Feed title: {}", dgfFeed.title);
                    dgfFeed.mapFeedSource(source);

                    source.setName(dgfFeed.title);

                    source.setProject(project);

                    source.save();

                    // create / update the properties

                    for(Field dgfField : dgfFeed.getClass().getDeclaredFields()) {
                        String fieldName = dgfField.getName();
                        String fieldValue = dgfField.get(dgfFeed) != null ? dgfField.get(dgfFeed).toString() : null;

                        ExternalFeedSourceProperty.updateOrCreate(source, this.getResourceType(), fieldName, fieldValue);
                    }
                }
            } catch (Exception ex) {
                LOG.error("Error reading from datagouvfr API");
                ex.printStackTrace();
            }
        }
        while(!next_page.contentEquals("null"));

    }

    @Override
    public void feedSourceCreated(FeedSource source, String authHeader) {

    }

    @Override
    public void propertyUpdated(ExternalFeedSourceProperty property, String previousValue, String authHeader) {

    }

    @Override
    public void feedVersionCreated(FeedVersion feedVersion, String authHeader) {

    }
}
