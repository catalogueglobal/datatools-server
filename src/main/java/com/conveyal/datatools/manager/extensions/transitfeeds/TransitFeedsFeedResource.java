package com.conveyal.datatools.manager.extensions.transitfeeds;

import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.extensions.ExternalFeedResource;
import com.conveyal.datatools.manager.models.ExternalFeedSourceProperty;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static spark.Spark.halt;

import static com.conveyal.datatools.manager.models.ExternalFeedSourceProperty.constructId;

/**
 * Created by demory on 3/31/16.
 */
public class TransitFeedsFeedResource implements ExternalFeedResource {

    private static final String apiKey = DataManager.getConfigPropertyAsText("extensions.transitfeeds.key");

    public TransitFeedsFeedResource () {}

    @Override
    public String getResourceType() {
        return "TRANSITFEEDS";
    }

    @Override
    public void importFeedsForProject(Project project, String authHeader) {
        LOG.info("Importing feeds from TransitFeeds");
        // multiple pages for TransitFeeds because of 100 feed limit per page
        Boolean nextPage = true;
        // page count starts at 1
        int page = 1;

        do {
            Map<String, String> params = new HashMap<>();
            params.put("key", apiKey);
            params.put("limit", String.valueOf(100));
            params.put("page", String.valueOf(page));
            params.put("type", "gtfs");
            String json = getFeedsJson(getUrl(params), null);
            if (json == null) return;

            JsonNode transitFeedNode;
            try {
                transitFeedNode = mapper.readTree(json);
            } catch (IOException ex) {
                LOG.error("Error parsing TransitFeeds JSON response");
                halt(400);
                return;
            }

            // iterate over TransitFeeds json
            for (JsonNode feed : transitFeedNode.get("results").get("feeds")) {
                TransitFeedsFeed externalFeed = constructTransitFeed(project, feed);
                if (externalFeed != null) {
                    System.out.println(externalFeed.getId());
                    importFeed(project, externalFeed);
                }
            }
            if (transitFeedNode.get("results").get("page") == transitFeedNode.get("results").get("numPages")){
                LOG.info("finished last page of transitfeeds");
                nextPage = false;
            }
            page++;
        } while(nextPage);
    }

    /**
     * TransitFeeds API is nested, so we just manually construct feeds rather than worry about complicated
     * deserialization. API docs: http://transitfeeds.com/api/swagger/#!/default/getFeeds
     * @param project
     * @param json
     * @return
     */
    private TransitFeedsFeed constructTransitFeed(Project project, JsonNode json) {
        // test that feed falls in bounding box (if box exists)
        if (project.bounds != null) {
            double lat = json.get("l").get("lat").asDouble();
            double lng = json.get("l").get("lng").asDouble();
            if (lat < project.bounds.south || lat > project.bounds.north || lng < project.bounds.west || lng > project.bounds.east) {
                return null;
            }
        }

        String feedId, feedName, feedUrl = null;
        feedId = json.get("id").asText();
        feedName = json.get("t").asText();

        if (json.get("u") != null) {
            if (json.get("u").get("d") != null) {
                feedUrl = json.get("u").get("d").asText();
            } else if (json.get("u").get("i") != null) {
                feedUrl = json.get("u").get("i").asText();
            }
        }
        return new TransitFeedsFeed(feedId, feedName, feedUrl);
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
