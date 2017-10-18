package com.conveyal.datatools.manager.persistence;

import com.conveyal.datatools.manager.models.FeedVersion;
import com.conveyal.gtfs.GTFSFeed;
import com.conveyal.r5.transit.TransportNetwork;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * This cache manages r5 TransportNetworks that are associated with FeedVersions in the application. There is a
 * TransportNetworkCache in r5, but it functions in a manner specific to analysis, so we need a special class here.
 *
 * WARNING: this is not necessarily built for scalable use of isochrone generation in the application, but rather as an
 * experimental approach to quickly generate isochrones for a GTFS feed.
 */
public class TransportNetworkCache {
    private static final Logger LOG = LoggerFactory.getLogger(TransportNetworkCache.class);

//    private final Weigher<String, TransportNetwork> weigher = (k, transportNetwork) ->
//            transportNetwork.gridPointSet.featureCount();
    private final LoadingCache<String, TransportNetwork> transportNetworkCache;
    private final Set<String> loadedTransportNetworks = new HashSet<>();

    /**
     * Listens for removal from cache (due to expiration or size restriction) and sets transportNetwork to null for GC.
     */
    private final RemovalListener<String, GTFSFeed> removalListener = removalNotification -> {
        // Set version transportNetwork to null on removal to initiate garbage collection.
        // FIXME: is there a better way to initiate gc?
        String feedVersionId = removalNotification.getKey();
        LOG.info("Evicting transport network. Cause: {}; ID: {}", removalNotification.getCause(), feedVersionId);
        FeedVersion version = FeedVersion.get(feedVersionId);
        loadedTransportNetworks.remove(feedVersionId);
    };

    public TransportNetworkCache () {
         transportNetworkCache = CacheBuilder.newBuilder()
                 // we use SoftReferenced values because we have the constraint that we don't want more than one
                 // copy of a particular TransportNetwork object around; that would mean multiple MapDBs are pointing
                 // at the same file, which is bad.
                 // TODO: should we be using soft values?
                .softValues()
                .expireAfterAccess(10, TimeUnit.MINUTES)
                 // TODO: Use maximumWeight instead of using maximumSize for better resource consumption estimate?
//            .maximumWeight(100000)
//            .weigher(weigher)
                .maximumSize(3)
                .removalListener(removalListener)
                .build(new CacheLoader() {
                    @Override
                    public TransportNetwork load(Object key) throws Exception {
                        // Thanks, java, for making me use a cast here. If I put generic arguments to new CacheLoader
                        // due to type erasure it can't be sure I'm using types correctly.
                        FeedVersion version = FeedVersion.get((String) key);
                        if (version != null) {
                            return version.buildOrReadTransportNetwork();
                        } else {
                            LOG.error("Version does not exist for id {}", key);
                            // This throws a CacheLoader$InvalidCacheLoadException
                            return null;
                        }
//                    return FeedVersionController.buildOrReadTransportNetwork(FeedVersion.get((String) key), null);
                    }
                });
    }

    /**
     * Wraps get method on cache to handle any exceptions.
     */
    public TransportNetwork getTransportNetwork (String feedVersionId) throws ExecutionException {
        try {
            loadedTransportNetworks.add(feedVersionId);
            TransportNetwork tn = transportNetworkCache.get(feedVersionId);
            return tn;
        } catch (Exception e) {
            LOG.error("Could not read or build transport network for {}", feedVersionId);
            e.printStackTrace();
            throw e;
        }
    }

    public boolean containsTransportNetwork (String feedVersionId) {
        return loadedTransportNetworks.contains(feedVersionId);
    }
}
