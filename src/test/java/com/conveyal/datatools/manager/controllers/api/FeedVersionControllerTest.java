package com.conveyal.datatools.manager.controllers.api;

import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.models.Project;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import static io.restassured.RestAssured.given;
import static io.restassured.module.jsv.JsonSchemaValidator.matchesJsonSchemaInClasspath;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

public class FeedVersionControllerTest extends DatatoolsTest {
    private static final Logger LOG = LoggerFactory.getLogger(FeedVersionControllerTest.class);
    private Project testProject;
    private FeedSource testFeedSource;
    private static boolean setUpIsDone = false;

    @After
    public void tearDown() {
        testFeedSource.delete();
        testProject.delete();
    }

    @Before
    public void setUp() {
        if (setUpIsDone) {
            return;
        }
        super.setUp();
        LOG.info("FeedVersionControllerTest setup");
        testProject = new Project();
        testProject.save();
        testFeedSource = new FeedSource("test-feed-source");
        testFeedSource.setProject(testProject);
        setUpIsDone = true;
    }

    /**
     * Make sure that the essential CRUD operations of the Feed Version Controller work without errors
     */
    @Test
    public void canCreateEditAndDeleteAFeedVersion() {
        String resourcesPath = "src/test/resources/com/conveyal/datatools/";
        String feedVersionJsonSchema = "com/conveyal/datatools/feed-version-schema.json";

        // can create the feed version
        FeedVersionDto feedVersionToCreate = new FeedVersionDto();

        given()
            .port(4000)
            .multiPart(new File(resourcesPath + "caltrain_gtfs.zip"))
            .post("/api/manager/secure/feedversion?feedSourceId=" + testFeedSource.id + "&lastModified=1508291519372")
        .then()
            .statusCode(200)
            .body(containsString("true"));

        // creating a feed will kick off some jobs to process.
        // Those jobs will fail but they are not needed for the purposes of this test

        // can find the new feed version in list of all feed versions
        FeedVersionDto createdFeedVersion = getFeedVersionsInFeedSource(testFeedSource.id).get(0);

        // can find the new feed version in request for newly created feed version
        given()
            .port(4000)
            .get("/api/manager/secure/feedversion/" + createdFeedVersion.id)
        .then()
            .body(matchesJsonSchemaInClasspath(feedVersionJsonSchema))
            .body("id", equalTo(createdFeedVersion.id));

        // can change update the created feed version by changing the name of the feed version
        // make put request, but this endpoint doesn't return the model
        given()
            .port(4000)
            .put("/api/manager/secure/feedversion/" + createdFeedVersion.id + "/rename?name=changed-name")
        .then()
            .body(containsString("true"));

        // make another request to verify persistance of updated name
        given()
            .port(4000)
            .get("/api/manager/secure/feedversion/" + createdFeedVersion.id)
        .then()
            .body(matchesJsonSchemaInClasspath(feedVersionJsonSchema))
            .body("name", equalTo("changed-name"));

        // can delete the updated feed version
        given()
            .port(4000)
            .delete("/api/manager/secure/feedversion/" + createdFeedVersion.id)
        .then()
            .body(matchesJsonSchemaInClasspath(feedVersionJsonSchema));

        // the feed version is not found in webservice call for all feed versions
        assertThat(feedVersionExistsInCollection(testFeedSource.id, createdFeedVersion.id), equalTo(false));
    }

    private boolean feedVersionExistsInCollection(String feedSourceId, String feedVersionId) {
        List<FeedVersionDto> feedVersions = getFeedVersionsInFeedSource(feedSourceId);

        boolean foundFeedVersion = false;
        for (FeedVersionDto feedVersion: feedVersions) {
            if (feedVersion.id.equals(feedVersionId)) {
                foundFeedVersion = true;
                break;
            }
        }

        return foundFeedVersion;
    }

    private List<FeedVersionDto> getFeedVersionsInFeedSource(String feedSourceId) {
        return Arrays.asList(
            given()
                .port(4000)
                .get("/api/manager/secure/feedversion?feedSourceId=" + feedSourceId)
                .as(FeedVersionDto[].class)
        );
    }
}
