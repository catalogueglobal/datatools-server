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
import spark.Request;
import spark.Response;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Map;

import static com.conveyal.datatools.manager.models.ExternalFeedSourceProperty.constructId;

/**
 * This interface allows for the creation of and interaction with external feed resources
 * (e.g., indexes of URLs for GTFS).
 */
public interface ExternalFeedResource {
    Logger LOG = LoggerFactory.getLogger(ExternalFeedResource.class);
    ObjectMapper mapper = new ObjectMapper();

    String getResourceType();

    default URL getUrl() {
        return getUrl(null);
    }

    default URL getUrl(Map<String, String> queryParams) {
        URIBuilder b;
        try {
            b = new URIBuilder(getBaseUrl());
            if (queryParams != null) queryParams.forEach((key, value) -> b.addParameter(key, value));
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
        return DataManager.getConfigPropertyAsText(String.join(".", "extensions", getResourceType().toLowerCase(), "api"));
    }

    // TODO? add default importFeeds method that accounts for polymorphic deserialization from JSON response: https://github.com/FasterXML/jackson-docs/wiki/JacksonPolymorphicDeserialization
    void importFeedsForProject(Project project, String authHeader);
//    {
//        LOG.info("Importing external feeds");
//        try {
//            String json = getFeedsJson(getUrl(), authHeader);
//            if (json == null) {
//                LOG.warn("No JSON response found");
//                return;
//            }
//            mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_CONCRETE_AND_ARRAYS);
////            SimpleAbstractTypeResolver resolver = new SimpleAbstractTypeResolver();
////            resolver.addMapping(ExternalFeed.class, RtdCarrier.class);
////            resolver.addMapping(ExternalFeed.class, TransitFeedsFeed.class);
////            resolver.addMapping(ExternalFeed.class, TransitLandFeed.class);
//            ExternalFeed[] results = mapper.readValue(json, ExternalFeed[].class);
//            for (ExternalFeed externalFeed : results) {
//                System.out.println(externalFeed.getClass().getSimpleName());
//                importFeed(project, externalFeed);
//            }
//        } catch(IOException e) {
//            LOG.error("Could not read feeds from MTC RTD API");
//            e.printStackTrace();
//        }
//    }

    default void feedSourceCreated(FeedSource source, String authHeader) {
        LOG.info("Processing new FeedSource {} for {}", source.name, getResourceType());
    }

    void propertyUpdated(ExternalFeedSourceProperty property, String previousValue, String authHeader);

    /**
     * When a new feed version for a feed source is PUBLISHED (not necessarily just created)
     * this function should be called. Publishing happens at
     * {@link com.conveyal.datatools.manager.controllers.api.FeedVersionController#publishToExternalResource(Request, Response)}.
     */
    default void feedVersionCreated(FeedVersion feedVersion, String authHeader) {
        LOG.info("feed version created! (no further action taken)");
    }

    /**
     * Default method for checking whether an external feed source already exists in the project (using the
     * getId and getIdKey interface methods).
     * @param project
     * @param externalFeed
     * @return
     */
    default FeedSource checkForExistingFeed(Project project, ExternalFeed externalFeed) {
        // check if a feed already exists with this id
        for (FeedSource existingSource : project.retrieveProjectFeedSources()) {
            String id = constructId(existingSource, this.getResourceType(), externalFeed.getIdKey());
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
     * @param url
     * @param authHeader
     * @return
     */
    default String getFeedsJson (URL url, String authHeader) {
        if (url == null) {
            LOG.warn("URL must not be null");
            return null;
        }

        StringBuilder response = new StringBuilder();
        try {
            HttpURLConnection con = (HttpURLConnection) url.openConnection();

            // optional default is GET
            con.setRequestMethod("GET");

            //add request header
            con.setRequestProperty("User-Agent", "User-Agent");

            if (authHeader != null) {
                // add auth header
                LOG.info("authHeader=" + authHeader);
                con.setRequestProperty("Authorization", authHeader);
            }


            LOG.info("\nSending 'GET' request to URL : {}", url);
            LOG.info("Response: {}", con.getResponseCode());

            BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
            String inputLine;

            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            in.close();
        } catch (IOException ex) {
            LOG.error("Could not read from {} API", getResourceType());
            ex.printStackTrace();
            return null;
        }

        return response.toString();
    }

    /**
     * Default method for importing an external feed resource into a project.
     * @param project
     * @param externalFeed
     */
    default void importFeed(Project project, ExternalFeed externalFeed) {
        boolean feedSourceExistsAlready = false;
        // check for existing feed in project (see if unique ID already exists)
        FeedSource source = checkForExistingFeed(project, externalFeed);

        String feedName = externalFeed.getName();

        if (source == null) {
            feedSourceExistsAlready = true;
            source = new FeedSource(feedName);
            LOG.info("Creating new feed source: {}", source.name);
        } else {
            LOG.info("Syncing properties: {}", source.name);
        }
        // if feed has feed URL, set here and set to auto fetch by URL
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
            Persistence.feedSources.replace(source.id, source);
        } else {
            // Create new feed source
            Persistence.feedSources.create(source);
        }

        // create / update the properties using reflection
        for(Field externalField : externalFeed.getClass().getDeclaredFields()) {
            String fieldName = externalField.getName();
            String fieldValue = null;
            try {
                fieldValue = externalField.get(externalFeed) != null
                    ? externalField.get(externalFeed).toString()
                    : null;
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
            ExternalFeedSourceProperty prop = new ExternalFeedSourceProperty(source, getResourceType(), fieldName, fieldValue);
            // Update or create property.
            if (Persistence.externalFeedSourceProperties.getById(prop.id) == null) {
                Persistence.externalFeedSourceProperties.create(prop);
            } else {
                Persistence.externalFeedSourceProperties.updateField(prop.id, fieldName, fieldValue);
            }
        }
    }
}
