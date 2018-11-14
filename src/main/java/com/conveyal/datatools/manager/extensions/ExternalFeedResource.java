package com.conveyal.datatools.manager.extensions;

import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.models.ExternalFeedSourceProperty;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.Project;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.client.utils.URIBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Map;

/**
 * This interface allows for the creation of and interaction with external feed resources
 * (e.g., indexes of URLs for GTFS).
 */
public interface ExternalFeedResource {
    Logger LOG = LoggerFactory.getLogger(ExternalFeedResource.class);
    ObjectMapper mapper = new ObjectMapper();

    /**
     * Method that should return the resource type, which is used to register the extension and is recorded in
     * {@link ExternalFeedSourceProperty} records.
     * */
    String getResourceType();

    /** Wrapper method for getUrl with no query parameters */
    default URL getUrl() {
        return getUrl(null);
    }

    /** Get external API URL with appended query parameters */
    default URL getUrl(Map<String, String> queryParams) {
        URIBuilder b;
        try {
            b = new URIBuilder(getBaseUrl());
            if (queryParams != null) queryParams.forEach(b::addParameter);
            return b.build().toURL();
        } catch (URISyntaxException | MalformedURLException e) {
            LOG.error("Could not construct URL for {} API", getResourceType());
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Gets API base URL as string from config property key "extensions.data_provider.api". Note: config property
     * key is derived from getResourceType (converted to all lowercase characters).
     */
    default String getBaseUrl() {
        String type = getResourceType().toLowerCase();
        return DataManager.getConfigPropertyAsText(String.join(".", "extensions", type, "api"));
    }

    /**
     * Map a JSON string to an external feed subclass. Note: this should only be used in
     * {@link #importFeedsForProject(Project, String)}.
     * */
    ExternalFeed[] mapJsonToExternalFeed(String json) throws IOException;

    /**
     * Default method for importing/syncing feeds from external resource. NOTE: Using this default method relies on
     * overriding the {@link #mapJsonToExternalFeed} that deserializes a JSON string into a subclass of {@link ExternalFeed}. For
     * more complicated integrations with external APIs, this method should be overridden.
     */
    default void importFeedsForProject(Project project, String authHeader) throws IOException {
        LOG.info("Importing {} feeds", getResourceType());
        String json = getExternalFeeds(getUrl(), authHeader);
        ExternalFeed[] results = mapJsonToExternalFeed(json);
        LOG.info("{} feeds found", results.length);
        for (ExternalFeed externalFeed : results) {
            importFeed(project, externalFeed);
        }
    }

    /**
     * Hook method for when a feed source is created in the Data Tools application (e.g., in order to POST the new feed
     * source record to an external service). Override this method to add custom logic.
     */
    void feedSourceCreated(FeedSource source, String authHeader);

    /**
     * Hook method for when an external property is updated in the Data Tools application (e.g., in order to PUT the
     * update to an external service). Override this method to add custom logic.
     */
    void propertyUpdated(ExternalFeedSourceProperty property, String previousValue, String authHeader);

    /**
     * When a new feed version for a feed source is PUBLISHED (not necessarily just created)
     * this hook method is called. Publishing itself happens at the private method `publishToExternalResource` in
     * {@link com.conveyal.datatools.manager.controllers.api.FeedVersionController}. Override this method to add custom
     * logic.
     */
    void feedVersionCreated(FeedVersion feedVersion, String authHeader);

    /**
     * Default method for checking whether an external feed source already exists in the project (using the
     * getId and getIdKey interface methods).
     */
    default FeedSource checkForExistingFeed(Project project, ExternalFeed externalFeed) {
        // check if a feed already exists with this id
        for (FeedSource existingSource : project.retrieveProjectFeedSources()) {
            String id = ExternalFeedSourceProperty.constructId(existingSource, getResourceType(), externalFeed.getIdKey());
            ExternalFeedSourceProperty prop = Persistence.externalFeedSourceProperties.getById(id);
            if (prop != null && prop.value != null && prop.value.equals(externalFeed.getId())) {
                return existingSource;
            }
        }
        return null;
    }

    /**
     * Default method for getting JSON string from external feeds resource. If authHeader is provided/non-null,
     * it will be used to authenticate against the server to which the request is made.
     */
    default String getExternalFeeds(URL url, String authHeader) throws IOException {
        StringBuilder response = new StringBuilder();
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        // Add request header
        con.setRequestProperty("User-Agent", "User-Agent");
        if (authHeader != null) {
            // Add auth header
            con.setRequestProperty("Authorization", authHeader);
        }
        LOG.info("External GET request: {}", url);
        LOG.info("Response: {}", con.getResponseCode());
        BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
        String inputLine;
        while ((inputLine = in.readLine()) != null) {
            response.append(inputLine);
        }
        in.close();
        return response.toString();
    }

    /**
     * Default method for importing an external feed resource into a project.
     */
    default void importFeed(Project project, ExternalFeed externalFeed) {
        boolean feedSourceExistsAlready = false;
        // check for existing feed in project (see if unique ID already exists)
        FeedSource source = checkForExistingFeed(project, externalFeed);

        String feedName = externalFeed.getName();

        if (source == null) {
            source = new FeedSource(feedName);
        } else {
            feedSourceExistsAlready = true;
        }
        // Set feed source properties.
        source.name = feedName;
        source.projectId = project.id;
        // If feed has feed URL, set here and set to auto fetch by URL
        String feedUrl = externalFeed.getFeedUrl();
        if (feedUrl != null) {
            try {
                source.url = new URL(feedUrl);
                source.retrievalMethod = FeedSource.FeedRetrievalMethod.FETCHED_AUTOMATICALLY;
            } catch (MalformedURLException e) {
                LOG.warn("Bad URL for {}: {}", feedName, feedUrl);
                e.printStackTrace();
            }
        }

        if (feedSourceExistsAlready) {
            // Update fields using replace
            LOG.info("Syncing properties: {}", source.name);
            Persistence.feedSources.replace(source.id, source);
        } else {
            // Create new feed source
            LOG.info("Creating new feed source: {}", source.name);
            Persistence.feedSources.create(source);
        }

        // Create / update the properties using reflection
        for(Field externalField : externalFeed.getClass().getDeclaredFields()) {
            String type = getResourceType();
            String name = externalField.getName();
            String value = null;
            try {
                value = externalField.get(externalFeed) != null
                    ? externalField.get(externalFeed).toString()
                    : null;
            } catch (IllegalAccessException e) {
                LOG.error("Could not get value for {} field {} on feed source {}", type, name, source.id);
                e.printStackTrace();
            }
            ExternalFeedSourceProperty prop = new ExternalFeedSourceProperty(source, type, name, value);
            // Update or create property.
            boolean propertyDoesNotExist = Persistence.externalFeedSourceProperties.getById(prop.id) == null;
            if (propertyDoesNotExist) Persistence.externalFeedSourceProperties.create(prop);
            else Persistence.externalFeedSourceProperties.updateField(prop.id, name, value);
        }
    }
}
