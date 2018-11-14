package com.conveyal.datatools.manager.extensions.transitfeeds;

import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.extensions.ExternalFeed;
import com.conveyal.datatools.manager.extensions.ExternalFeedResource;
import com.conveyal.datatools.manager.models.ExternalFeedSourceProperty;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.Project;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * A resource for importing and syncing feeds and their properties from the transitfeeds.com catalog.
 * Created by demory on 3/31/16.
 */
public class TransitFeedsFeedResource implements ExternalFeedResource {
    private Logger LOG = LoggerFactory.getLogger(TransitFeedsFeedResource.class);
    private static final String apiKey = DataManager.getConfigPropertyAsText("extensions.transitfeeds.key");

    public TransitFeedsFeedResource () {}

    @Override
    public String getResourceType() {
        return "TRANSITFEEDS";
    }

    @Override
    public ExternalFeed[] mapJsonToExternalFeed(String json) throws IOException {
        return new ExternalFeed[0];
    }

    @Override
    public void importFeedsForProject(Project project, String authHeader) throws IOException {
        LOG.info("Importing feeds from TransitFeeds");
        // multiple pages for TransitFeeds because of 100 feed limit per page
        // page count starts at 1
        final int feedsPerPage = 100;
        int currentPage = 1;
        int numPages;
        Map<String, String> params = new HashMap<>();
        params.put("key", apiKey);
        params.put("type", "gtfs");
        params.put("limit", String.valueOf(feedsPerPage));
        do {
            // Update page number on each iteration
            params.put("page", String.valueOf(currentPage));
            String json = getExternalFeeds(getUrl(params), null);
            JsonNode transitFeedNode = mapper.readTree(json);
            // iterate over TransitFeeds json
            JsonNode results = transitFeedNode.get("results");
            numPages = results.get("numPages").asInt();
            for (JsonNode feed : results.get("feeds")) {
                TransitFeedsFeed externalFeed = constructTransitFeed(project, feed);
                if (externalFeed != null) {
                    LOG.info("Importing feed id: {}", externalFeed.getId());
                    importFeed(project, externalFeed);
                }
            }
            currentPage++;
        } while(currentPage <= numPages);
        LOG.info("Finished last page for TransitFeeds");
    }

    @Override
    public void feedSourceCreated(FeedSource source, String authHeader) {
        // Do nothing.
    }

    @Override
    public void propertyUpdated(ExternalFeedSourceProperty property, String previousValue, String authHeader) {
        // Do nothing.
    }

    @Override
    public void feedVersionCreated(FeedVersion feedVersion, String authHeader) {
        // Do nothing.
    }

    /**
     * TransitFeeds API is nested, so we just manually construct feeds rather than worry about complicated
     * deserialization. API docs: http://transitfeeds.com/api/swagger/#!/default/getFeeds
     */
    private TransitFeedsFeed constructTransitFeed(Project project, JsonNode feedJson) {
        if (project.bounds != null) {
            // Test that feed falls in bounding box (if bounds exist for project)
            double lat = feedJson.get("l").get("lat").asDouble();
            double lng = feedJson.get("l").get("lng").asDouble();
            if (!project.bounds.containsCoordinate(lat, lng)) {
                // Skip feed if it falls outside the bounds.
                return null;
            }
        }

        String feedId, feedName, feedUrl = null;
        feedId = feedJson.get("id").asText();
        feedName = feedJson.get("t").asText();

        if (feedJson.get("u") != null) {
            if (feedJson.get("u").get("d") != null) {
                feedUrl = feedJson.get("u").get("d").asText();
            } else if (feedJson.get("u").get("i") != null) {
                feedUrl = feedJson.get("u").get("i").asText();
            }
        }
        return new TransitFeedsFeed(feedId, feedName, feedUrl);
    }
}
