package com.conveyal.datatools.manager.controllers.api;

import com.conveyal.datatools.DatatoolsTest;
import com.conveyal.datatools.manager.models.Project;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

import static io.restassured.RestAssured.given;
import static io.restassured.module.jsv.JsonSchemaValidator.matchesJsonSchemaInClasspath;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class FeedSourceControllerTest extends DatatoolsTest {
    private static final Logger LOG = LoggerFactory.getLogger(FeedSourceControllerTest.class);
    private Project testProject;
    private static boolean setUpIsDone = false;

    @After
    public void tearDown() {
        testProject.delete();
    }

    @Before
    public void setUp() {
        if (setUpIsDone) {
            return;
        }
        super.setUp();
        LOG.info("FeedSourceControllerTest setup");
        testProject = new Project();
        testProject.save();
        setUpIsDone = true;
    }

    /**
     * Make sure that the essential CRUD operations of the Feed Source Controller work without errors
     */
    @Test
    public void canCreateEditAndDeleteAFeedSource() {
        String feedSourceJsonSchema = "com/conveyal/datatools/feed-source-schema.json";

        // can create the feed source
        FeedSourceDto feedSourceToCreate = new FeedSourceDto();
        feedSourceToCreate.deployable = false;
        feedSourceToCreate.isPublic = false;
        feedSourceToCreate.name = "test-feed-source";
        feedSourceToCreate.projectId = testProject.id;
        feedSourceToCreate.retrievalMethod = "MANUALLY_UPLOADED";
        feedSourceToCreate.url = "http://example.com";

        FeedSourceDto createdFeedSource = given()
            .port(4000)
            .body(feedSourceToCreate)
            .post("/api/manager/secure/feedsource")
        .then()
            .body(matchesJsonSchemaInClasspath(feedSourceJsonSchema))
        .extract()
            .as(FeedSourceDto.class);

        assertThat(createdFeedSource.name, equalTo("test-feed-source"));

        // can find the new feed source in list of all feed sources
        assertThat(feedSourceExistsInCollection(createdFeedSource.id), equalTo(true));

        // can find the new feed source in request for newly created feed source
        given()
            .port(4000)
            .get("/api/manager/secure/feedsource/" + createdFeedSource.id)
        .then()
            .body(matchesJsonSchemaInClasspath(feedSourceJsonSchema))
            .body("id", equalTo(createdFeedSource.id));

        // can change update the created feed source by changing the name of the feed source
        createdFeedSource.name = "changed-name";
        given()
            .port(4000)
            .body(createdFeedSource)
            .put("/api/manager/secure/feedsource/" + createdFeedSource.id)
        .then()
            .body(matchesJsonSchemaInClasspath(feedSourceJsonSchema))
            .body("name", equalTo(createdFeedSource.name));

        // can delete the updated feed source
        given()
            .port(4000)
            .delete("/api/manager/secure/feedsource/" + createdFeedSource.id)
        .then()
            .body(matchesJsonSchemaInClasspath(feedSourceJsonSchema));

        // the feed source is not found in webservice call for all feed sources
        assertThat(feedSourceExistsInCollection(createdFeedSource.id), equalTo(false));
    }

    private boolean feedSourceExistsInCollection(String id) {
        List<FeedSourceDto> feedSources = Arrays.asList(
            given()
                .port(4000)
                .get("/api/manager/secure/feedsource")
                .as(FeedSourceDto[].class));

        boolean foundFeedSource = false;
        for (FeedSourceDto feedSource: feedSources) {
            if (feedSource.id.equals(id)) {
                foundFeedSource = true;
                break;
            }
        }

        return foundFeedSource;
    }
}
