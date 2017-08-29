package com.conveyal.datatools.manager.extensions;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Interface for external feeds that are to be imported into the data manager. The few methods here allow us to
 * compare feeds with another and get the common fields that should be mapped to internal feed sources.
 */

public interface ExternalFeed {

    String getFeedUrl();

    String getId();

    String getIdKey();

    String getName();
}
