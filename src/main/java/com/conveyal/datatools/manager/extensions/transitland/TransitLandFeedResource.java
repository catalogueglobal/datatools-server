package com.conveyal.datatools.manager.extensions.transitland;

import com.conveyal.datatools.manager.extensions.ExternalFeedResource;
import com.conveyal.datatools.manager.models.ExternalFeedSourceProperty;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.Project;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

/**
 * A resource for importing and syncing feeds and their properties from the transit.land catalog.
 * Created by demory on 3/31/16.
 */

public class TransitLandFeedResource implements ExternalFeedResource {

    public TransitLandFeedResource() { }

    @Override
    public String getResourceType() {
        return "TRANSITLAND";
    }

    @Override
    public TransitLandFeed[] mapJsonToExternalFeed(String json) throws IOException {
        return mapper.readValue(json, TransitLandFeed[].class);
    }

    @Override
    public void importFeedsForProject(Project project, String authHeader) throws IOException {
        LOG.info("Importing TransitLand feeds");
        int perPage = 10000;
        int count = 0;
        int offset;
        int total = 0;
        do {
            // Construct URL with query params.
            offset = perPage * count;
            Map<String, String> params = new HashMap<>();
            params.put("total", "true");
            params.put("per_page", String.valueOf(perPage));
            params.put("offset", String.valueOf(offset));
            // Add bounding box param if project has bounds.
            if (project.bounds != null) params.put("bbox", project.bounds.toTransitLandString());
            URL url = getUrl(params);
            // Make request and process JSON response.
            String json = getExternalFeeds(url, null);
            JsonNode node = mapper.readTree(json);
            // Get total number of feeds to determine if there are more results on the next page.
            if (count == 0) {
                total = node.get("meta").get("total").asInt();
                LOG.info("Total TransitLand feeds: " + total);
            }
            // map json node to TransitLandFeeds and import
            TransitLandFeed[] tlFeeds = mapJsonToExternalFeed(node.get("feeds").toString());
            LOG.info("{} feeds on page {}", tlFeeds.length, count);
            for (TransitLandFeed tlFeed : tlFeeds) {
                importFeed(project, tlFeed);
            }
            count++;

        } // Iterate over results until most recent total exceeds total feeds in TransitLand
        while(offset + perPage < total);
        LOG.info("Finished last page for TransitLand");
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
}
