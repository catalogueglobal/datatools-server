package com.conveyal.datatools.editor.controllers.api;

import com.conveyal.datatools.editor.controllers.Base;
import com.conveyal.datatools.editor.datastore.FeedTx;
import com.conveyal.datatools.editor.datastore.VersionedDataStore;
import com.conveyal.datatools.editor.models.transit.ScheduleException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collection;

import com.conveyal.datatools.manager.models.JsonViews;
import com.conveyal.datatools.manager.utils.json.JsonManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.HaltException;
import spark.Request;
import spark.Response;

import static spark.Spark.*;


public class ScheduleExceptionController {
    public static final JsonManager<ScheduleException> json =
            new JsonManager<>(ScheduleException.class, JsonViews.UserInterface.class);
    private static final Logger LOG = LoggerFactory.getLogger(ScheduleExceptionController.class);

    /** Get all of the schedule exceptions for an agency */
    public static Object getScheduleException (Request req, Response res) {
        String exceptionId = req.params("exceptionId");
        String feedId = req.queryParams("feedId");
        if (feedId == null)
            feedId = req.session().attribute("feedId");

        if (feedId == null) {
            halt(400);
        }

        FeedTx tx = null;

        try {
            tx = VersionedDataStore.getFeedTx(feedId);

            if (exceptionId != null) {
                if (!tx.exceptions.containsKey(exceptionId))
                    halt(400);
                else
                    return tx.exceptions.get(exceptionId);
            }
            else {
                return new ArrayList<>(tx.exceptions.values());
            }
        } catch (HaltException e) {
            LOG.error("Halt encountered", e);
            throw e;
        } catch (Exception e) {
            e.printStackTrace();
            halt(400);
        } finally {
            if (tx != null) tx.rollbackIfOpen();
        }
        return json;
    }
    
    public static Object createScheduleException (Request req, Response res) {
        FeedTx tx = null;
        try {
            ScheduleException ex = Base.mapper.readValue(req.body(), ScheduleException.class);

            if (!VersionedDataStore.feedExists(ex.feedId)) {
                halt(400);
            }

            if (req.session().attribute("feedId") != null && !req.session().attribute("feedId").equals(ex.feedId))
                halt(400);

            tx = VersionedDataStore.getFeedTx(ex.feedId);

            if (ex.customSchedule != null) {
                for (String cal : ex.customSchedule) {
                    if (!tx.calendars.containsKey(cal)) {
                        halt(400);
                    }
                }
            }
            if (ex.addedService != null) {
                for (String cal : ex.addedService) {
                    if (!tx.calendars.containsKey(cal)) {
                        halt(400);
                    }
                }
            }
            if (ex.removedService != null) {
                for (String cal : ex.removedService) {
                    if (!tx.calendars.containsKey(cal)) {
                        halt(400);
                    }
                }
            }

            if (tx.exceptions.containsKey(ex.id)) {
                halt(400);
            }
            checkDuplicateDates(ex, tx.exceptions.values());
            tx.exceptions.put(ex.id, ex);

            tx.commit();

            return ex;
        } catch (HaltException e) {
            LOG.error("Halt encountered", e);
            throw e;
        } catch (Exception e) {
            e.printStackTrace();
            halt(400);
        } finally {
            if (tx != null) tx.rollbackIfOpen();
        }
        return null;
    }
    
    public static Object updateScheduleException (Request req, Response res) {
        FeedTx tx = null;
        try {
            ScheduleException ex = Base.mapper.readValue(req.body(), ScheduleException.class);

            if (req.session().attribute("feedId") != null && !req.session().attribute("feedId").equals(ex.feedId))
                halt(400);

            if (!VersionedDataStore.feedExists(ex.feedId)) {
                halt(400);
            }

            tx = VersionedDataStore.getFeedTx(ex.feedId);

            if (ex.customSchedule != null) {
                for (String cal : ex.customSchedule) {
                    if (!tx.calendars.containsKey(cal)) {
                        halt(400);
                    }
                }
            }
            if (ex.addedService != null) {
                for (String cal : ex.addedService) {
                    if (!tx.calendars.containsKey(cal)) {
                        halt(400);
                    }
                }
            }
            if (ex.removedService != null) {
                for (String cal : ex.removedService) {
                    if (!tx.calendars.containsKey(cal)) {
                        halt(400);
                    }
                }
            }

            if (!tx.exceptions.containsKey(ex.id)) {
                halt(400);
            }

            checkDuplicateDates(ex, tx.exceptions.values());
            tx.exceptions.put(ex.id, ex);

            tx.commit();

            return ex;
        } catch (HaltException e) {
            LOG.error("Halt encountered", e);
            throw e;
        } catch (Exception e) {
            e.printStackTrace();
            halt(400);
        } finally {
            if (tx != null) tx.rollbackIfOpen();
        }
        return null;
    }

    /** Check that no dates for the schedule exception are repeated across all schedule exceptions for the feed. */
    private static void checkDuplicateDates(ScheduleException ex, Collection<ScheduleException> exceptions) {
        if (ex.dates != null) {
            for (LocalDate date : ex.dates) {
                // This is inefficient, but it keeps us from using the scheduleExceptionCountByDate map
                // which had significant issues with mapdb corruption likely due to the use of BindUtils.multiHistogram
                // which we believe is making an auto-updated view or triggers or something. It's likely that there's some
                // bug in combining these views with transactions.
                for (ScheduleException exception : exceptions) {
                    if (exception.dates.contains(date)) {
                        halt(400);
                    }
                }
            }
        }
    }

    public static Object deleteScheduleException (Request req, Response res) {
        String id = req.params("id");
        String feedId = req.queryParams("feedId");

        if (feedId == null)
            feedId = req.session().attribute("feedId");

        if (feedId == null) {
            halt(400);
        }
        FeedTx tx = null;
        try {
            tx = VersionedDataStore.getFeedTx(feedId);
            ScheduleException ex = tx.exceptions.get(id);
            tx.exceptions.remove(id);
            tx.commit();

            return ex; // ok();
        } catch (HaltException e) {
            LOG.error("Halt encountered", e);
            throw e;
        } catch (Exception e) {
            e.printStackTrace();
            halt(400);
        } finally {
            if (tx != null) tx.rollbackIfOpen();
        }
        return null;
    }

    public static void register (String apiPrefix) {
        get(apiPrefix + "secure/scheduleexception/:id", ScheduleExceptionController::getScheduleException, json::write);
        options(apiPrefix + "secure/scheduleexception", (q, s) -> "");
        get(apiPrefix + "secure/scheduleexception", ScheduleExceptionController::getScheduleException, json::write);
        post(apiPrefix + "secure/scheduleexception", ScheduleExceptionController::createScheduleException, json::write);
        put(apiPrefix + "secure/scheduleexception/:id", ScheduleExceptionController::updateScheduleException, json::write);
        delete(apiPrefix + "secure/scheduleexception/:id", ScheduleExceptionController::deleteScheduleException, json::write);
    }
}
