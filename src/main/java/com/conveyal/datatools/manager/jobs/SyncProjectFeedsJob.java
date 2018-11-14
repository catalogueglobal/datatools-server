package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.extensions.ExternalFeedResource;
import com.conveyal.datatools.manager.models.Project;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sync a project with an external resource (e.g., MTC's RTD or transit.land). This will either update the external
 * properties for existing feed sources that exist in the third party catalog or create new feed sources within the
 * project for feed sources that do not match on the external resource's unique feed IDs.
 */
public class SyncProjectFeedsJob extends MonitorableJob {

    private final Project project;
    private final String resourceType;
    private final String authHeader;
    private Logger LOG = LoggerFactory.getLogger(SyncProjectFeedsJob.class);

    public SyncProjectFeedsJob(String owner, Project project, String resourceType, String authHeader) {
        super(owner, String.format("Syncing %s feeds with %s", project.name, resourceType), JobType.SYNC_PROJECT_FEEDS);
        this.project = project;
        this.resourceType = resourceType;
        this.authHeader = authHeader;
    }

    @JsonProperty
    public String getProjectId () {
        return project.id;
    }

    @Override
    public void jobLogic() {
        try {
            status.update("Sync in progress...", 25);
            ExternalFeedResource resource = DataManager.feedResources.get(resourceType);
            resource.importFeedsForProject(project, authHeader);
            status.update("Sync complete!", 100);
        } catch (Exception e) {
            LOG.error("Could not complete sync job.", e);
            status.fail("Could not complete sync job", e);
        }
    }
}
