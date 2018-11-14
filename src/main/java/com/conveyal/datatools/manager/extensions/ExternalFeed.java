package com.conveyal.datatools.manager.extensions;

import com.conveyal.datatools.manager.models.FeedSource;

/**
 * Interface for external feeds that are to be imported into the data manager. The few methods here allow us to
 * compare feeds with another and get the common fields that should be mapped to internal feed sources.
 */
public interface ExternalFeed {
    /** The feed location URL (or null if there is none) according to the external resource. */
    String getFeedUrl();
    /** The unique identifier value according to the external resource. */
    String getId();
    /**
     * The name of the field containing the unique identifier (e.g., the string literal "onestop_id"). This value is
     * used when constructing the ID field in
     * {@link com.conveyal.datatools.manager.models.ExternalFeedSourceProperty#constructId(FeedSource, String, String)}.
     * */
    String getIdKey();
    /** The name of the feed according to the external resource. */
    String getName();
}
