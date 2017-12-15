package com.conveyal.datatools.manager.jobs;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.editor.models.Snapshot;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.FeedVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

import static com.conveyal.datatools.editor.models.Snapshot.writeSnapshotAsGtfs;

/**
 * Created by demory on 7/27/16.
 */
public class CreateFeedVersionFromSnapshotJob extends MonitorableJob {
    public static final Logger LOG = LoggerFactory.getLogger(CreateFeedVersionFromSnapshotJob.class);

    public FeedVersion feedVersion;
    private String snapshotId;

    public CreateFeedVersionFromSnapshotJob(FeedVersion feedVersion, String snapshotId, String owner) {
        super(owner, "Creating Feed Version from Snapshot for " + feedVersion.parentFeedSource().name, JobType.CREATE_FEEDVERSION_FROM_SNAPSHOT);
        this.feedVersion = feedVersion;
        this.snapshotId = snapshotId;
        status.message = "Initializing...";
    }

    @Override
    public void jobLogic() {
        // Process feed version once GTFS file written.
        addNextJob(new ProcessSingleFeedJob(feedVersion, owner));

        File file = null;

        try {
            // Write snapshot as GTFS file.
            file = File.createTempFile("snapshot", ".zip");
            writeSnapshotAsGtfs(snapshotId, file);
        } catch (Exception e) {
            e.printStackTrace();
            String message = "Unable to create temp file for snapshot";
            LOG.error(message);
            status.update(true, message, 100, true);
        }

        try {
            Snapshot snapshot = Snapshot.get(snapshotId);
            if (snapshot == null) {
                throw new IllegalArgumentException("Snapshot ID " + snapshotId + " does not exist");
            }
            feedVersion.updateFields(snapshot.name + " Snapshot Export", file, FeedSource.FeedRetrievalMethod.PRODUCED_IN_HOUSE);
        } catch (Exception e) {
            String message = "Unable to open input stream from upload";
            LOG.error(message);
            status.update(true, message, 100, true);
        }
        status.update(false, "Version created successfully.", 100, true);

        // ProcessSingleFeedJob will now be executed. Storing the FeedVersion in MongoDB will happen at the completion
        // of the Load/Validate tasks.
    }
}