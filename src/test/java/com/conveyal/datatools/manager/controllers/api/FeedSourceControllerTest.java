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
     * Make sure that the essential CRUD operations of the Project Controller work without errors
     */
    @Test
    public void canCreateEditAndDeleteAFeedSource() {
        String feedSourceJsonSchema = "com/conveyal/datatools/feed-source-schema.json";

        // can create the project
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

        // can find the new project in list of all projects
        assertThat(feedSourceExistsInCollection(createdFeedSource.id), equalTo(true));

        // can find the new project in request for newly created project
        given()
            .port(4000)
            .get("/api/manager/secure/feedsource/" + createdFeedSource.id)
        .then()
            .body(matchesJsonSchemaInClasspath(feedSourceJsonSchema))
            .body("id", equalTo(createdFeedSource.id));

        // can change update the created project by changing the name of the project
        createdFeedSource.name = "changed-name";
        given()
            .port(4000)
            .body(createdFeedSource)
            .put("/api/manager/secure/feedsource/" + createdFeedSource.id)
        .then()
            .body(matchesJsonSchemaInClasspath(feedSourceJsonSchema))
            .body("name", equalTo(createdFeedSource.name));

        // can delete the updated project
        given()
            .port(4000)
            .delete("/api/manager/secure/feedsource/" + createdFeedSource.id)
        .then()
            .body(matchesJsonSchemaInClasspath(feedSourceJsonSchema));

        // the project is not found in webservice call for all projects
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
