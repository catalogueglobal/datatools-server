package com.conveyal.datatools.manager.jobs;

import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.conveyal.datatools.manager.DataManager;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.conveyal.datatools.manager.persistence.FeedStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This
 */
public class FeedUpdater {
    private Map<String, String> eTags;

    public static final Logger LOG = LoggerFactory.getLogger(FeedUpdater.class);
    private final FeedStore feedStore;


    public FeedUpdater(FeedStore feedStore, int delay, int seconds) {
        this.feedStore = feedStore;
        this.eTags = new HashMap<>();
        LOG.info("Setting feed update to check every {} seconds. Beginning in {} seconds", seconds, delay);
        DataManager.scheduler.scheduleAtFixedRate(new UpdateFeedsTask(), delay, seconds, TimeUnit.SECONDS);
    }

    public void addFeedETags(Map<String, String> eTagList){
        this.eTags.putAll(eTagList);
    }

    class UpdateFeedsTask implements Runnable {
        public void run() {
            Map<String, String> updatedTags;
            boolean feedsWereUpdated = false;
            try {
                updatedTags = registerS3Feeds(eTags);
                feedsWereUpdated = !updatedTags.isEmpty();
                addFeedETags(updatedTags);
            } catch (Exception any) {
                LOG.warn("Error updating feeds {}", any);
            }
            if (feedsWereUpdated) {
                LOG.info("New eTag list " + eTags);
            }
            // TODO: compare current list of eTags against list in completed folder

            // TODO: load feeds for any feeds with new eTags
//            ApiMain.loadFeedFromBucket()
        }
    }

    /**
     * Used to register eTags (AWS file hash) of remote feeds in order to keep data-tools
     * application in sync with any external processes (for example, MTC RTD).
     * @param eTags
     * @return map of feedIDs to eTag values
     */
    public Map<String, String> registerS3Feeds (Map<String, String> eTags) {
        if (eTags == null) {
            eTags = new HashMap<>();
        }
        Map<String, String> newTags = new HashMap<>();
        // iterate over feeds in download_prefix folder and register to gtfsApi (MTC project)
        ObjectListing gtfsList = feedStore.listS3Feeds();
        for (S3ObjectSummary objSummary : gtfsList.getObjectSummaries()) {

            String eTag = objSummary.getETag();
            if (!eTags.containsValue(eTag)) {
                String keyName = objSummary.getKey();

                // Do not register object if it is a directory
                if (keyName.equals(feedStore.s3Prefix)){
                    continue;
                }
                String filename = keyName.split("/")[1];
                String feedId = filename.replace(".zip", "");
                try {
                    LOG.warn("New version found for " + keyName + ". Downloading from s3...");
                    // Trigger download to local disk
                    feedStore.getFeed(filename, true);

                    // delete old mapDB files
                    String[] dbFiles = {".db", ".db.p"};
                    for (String type : dbFiles) {
                        File db = new File(DataManager.storageDirectory, feedId + type);
                        db.delete();
                    }
                    // Put Etag for feedId into map for tracking
                    newTags.put(feedId, eTag);

                    // initiate load of feed source into API with retrieve call
                    // FIXME: update for sql-loading. Feed should already be in database, so instead of triggering a
                    // download and putting the feed in GTFSCache, we should simply check which feed in the DB (mongo or postgres?) matches the ETag.
                    // When we find a match, we should mark a field in the FeedSource for the given feed (something like
                    // processedVersionId... or should it be namespace?).
                    // If there is no match (that should be impossible) what do we do?
//                    ApiMain.getFeedSource(feedId);
                } catch (Exception e) {
                    LOG.warn("Could not load feed " + keyName, e);
                }
            }
        }
        return newTags;
    }

}
