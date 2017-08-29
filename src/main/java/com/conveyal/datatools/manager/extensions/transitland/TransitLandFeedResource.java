package com.conveyal.datatools.manager.extensions.transitland;

import com.conveyal.datatools.manager.extensions.ExternalFeedResource;
import com.conveyal.datatools.manager.models.ExternalFeedSourceProperty;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.Project;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by demory on 3/31/16.
 */

public class TransitLandFeedResource implements ExternalFeedResource {

    public TransitLandFeedResource() { }

    @Override
    public String getResourceType() {
        return "TRANSITLAND";
    }

    @Override
    public void importFeedsForProject(Project project, String authHeader) {
        LOG.info("Importing TransitLand feeds");
        int perPage = 10000;
        int count = 0;
        int offset;
        int total = 0;
        boolean nextPage = true;

        do {
            offset = perPage * count;
            Map<String, String> params = new HashMap<>();
            params.put("total", "true");
            params.put("per_page", String.valueOf(perPage));
            params.put("offset", String.valueOf(offset));
            if (project.north != null && project.south != null && project.east != null && project.west != null)
                params.put("bbox", project.west + "," + + project.south + "," + project.east + "," + project.north);

            try {
                String json = getFeedsJson(getUrl(params), null);
                JsonNode node = mapper.readTree(json);

                // get total number of feeds for calculating whether there are more results on the next page
                if (count == 0) {
                    total = node.get("meta").get("total").asInt();
                    LOG.info("Total TransitLand feeds: " + total);
                }

                // map json node to TransitLandFeeds and import
                List<TransitLandFeed> tlFeeds = mapper.readValue(mapper.treeAsTokens(node.get("feeds")), new TypeReference<List<TransitLandFeed>>(){});
                LOG.info("{} feeds on page {}", tlFeeds.size(), count);
                for (TransitLandFeed tlFeed : tlFeeds) {
                    importFeed(project, tlFeed);
                }

            } catch (Exception ex) {
                LOG.error("Error reading from TransitLand API");
                ex.printStackTrace();
            }

            // iterate over results until most recent total exceeds total feeds in TransitLand
            if (offset + perPage >= total){
                LOG.info("finished last page of TransitLand");
                nextPage = false;
            }
            count++;
        } while(nextPage);

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
