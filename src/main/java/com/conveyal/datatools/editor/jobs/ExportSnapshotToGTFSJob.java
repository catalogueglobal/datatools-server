package com.conveyal.datatools.editor.jobs;

import com.conveyal.datatools.common.status.MonitorableJob;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.datatools.manager.models.Snapshot;
import com.conveyal.datatools.manager.persistence.FeedStore;
import com.conveyal.gtfs.loader.FeedLoadResult;
import com.conveyal.gtfs.loader.JdbcGtfsExporter;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class ExportSnapshotToGTFSJob extends MonitorableJob {

    private static final Logger LOG = LoggerFactory.getLogger(ExportSnapshotToGTFSJob.class);
    private final Snapshot snapshot;
    private final String feedVersionId;

    public ExportSnapshotToGTFSJob(String owner, Snapshot snapshot, String feedVersionId) {
        super(owner, "Exporting snapshot " + snapshot.name, JobType.EXPORT_SNAPSHOT_TO_GTFS);
        this.snapshot = snapshot;
        this.feedVersionId = feedVersionId;
    }

    public ExportSnapshotToGTFSJob(String owner, Snapshot snapshot) {
        this(owner, snapshot, null);
    }

    @JsonProperty
    public Snapshot getSnapshot () {
        return snapshot;
    }

    @Override
    public void jobLogic() {
        File tempFile;
        try {
            tempFile = File.createTempFile("snapshot", "zip");
        } catch (IOException e) {
            e.printStackTrace();
            status.fail("Error creating local file for snapshot.", e);
            return;
        }
        JdbcGtfsExporter exporter = new JdbcGtfsExporter(snapshot.namespace, tempFile.getAbsolutePath(), DataManager.GTFS_DATA_SOURCE, true);
        FeedLoadResult result = exporter.exportTables();
        if (result.fatalException != null) {
            String message = String.format("Error (%s) encountered while exporting database tables.", result.fatalException);
            LOG.error(message);
            status.fail(message);
        }

        // Override snapshot ID if exporting feed for use as new feed version.
        String filename = feedVersionId != null ? feedVersionId : snapshot.id + ".zip";
        String bucketPrefix = feedVersionId != null ? "gtfs" : "snapshots";
        // FIXME: replace with use of refactored FeedStore.
        // Store the project merged zip locally or on s3
        if (DataManager.useS3) {
            String s3Key = String.format("%s/%s", bucketPrefix, filename);
            FeedStore.s3Client.putObject(DataManager.feedBucket, s3Key, tempFile);
            LOG.info("Storing snapshot GTFS at s3://{}/{}", DataManager.feedBucket, s3Key);
        } else {
            try {
                FeedVersion.feedStore.newFeed(filename, new FileInputStream(tempFile), null);
            } catch (IOException e) {
                LOG.error("Could not store feed for snapshot {}", snapshot.id);
                e.printStackTrace();
                status.fail("Could not export snapshot to GTFS.");
            }
        }
        // Delete snapshot temp file.
        tempFile.delete();
    }
}
