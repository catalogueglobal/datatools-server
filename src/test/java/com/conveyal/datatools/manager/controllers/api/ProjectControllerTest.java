package com.conveyal.datatools.manager.controllers.api;

import com.conveyal.datatools.DatatoolsTest;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static io.restassured.RestAssured.given;
import static io.restassured.module.jsv.JsonSchemaValidator.matchesJsonSchemaInClasspath;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class ProjectControllerTest extends DatatoolsTest {

    /**
     * Make sure that the essential CRUD operations of the Project Controller work without errors
     */
    @Test
    public void canCreateEditAndDeleteAProject() {
        String projectJsonSchema = "com/conveyal/datatools/project-schema.json";

        // can create the project
        ProjectDto projectToCreate = new ProjectDto();
        projectToCreate.autoFetchFeeds = false;
        projectToCreate.autoFetchHour = 21;
        projectToCreate.autoFetchMinute = 21;
        projectToCreate.defaultLanguage = "es";
        projectToCreate.defaultTimeZone = "Arctic/Longyearbyen";
        projectToCreate.defaultLocationLat = 0;
        projectToCreate.defaultLocationLon = 0;
        projectToCreate.east = 20;
        projectToCreate.name = "test-project";
        projectToCreate.north = 20;
        projectToCreate.organizationId = "test";
        projectToCreate.osmEast = 20;
        projectToCreate.osmNorth = 20;
        projectToCreate.osmSouth = -20;
        projectToCreate.osmWest = -20;
        projectToCreate.south = -20;
        projectToCreate.west = -20;
        projectToCreate.useCustomOsmBounds = false;

        ProjectDto createdProject = given()
            .port(4000)
            .body(projectToCreate)
            .post("/api/manager/secure/project")
        .then()
            .body(matchesJsonSchemaInClasspath(projectJsonSchema))
        .extract()
            .as(ProjectDto.class);

        assertThat(createdProject.name, equalTo("test-project"));

        // can find the new project in list of all projects
        assertThat(projectExistsInCollection(createdProject.id), equalTo(true));

        // can find the new project in request for newly created project
        given()
            .port(4000)
            .get("/api/manager/secure/project/" + createdProject.id)
        .then()
            .body(matchesJsonSchemaInClasspath(projectJsonSchema))
            .body("id", equalTo(createdProject.id));

        // can change update the created project by changing the name of the project
        createdProject.name = "changed-name";
        given()
            .port(4000)
            .body(createdProject)
            .put("/api/manager/secure/project/" + createdProject.id)
        .then()
            .body(matchesJsonSchemaInClasspath(projectJsonSchema))
            .body("name", equalTo(createdProject.name));

        // can delete the updated project
        given()
            .port(4000)
            .delete("/api/manager/secure/project/" + createdProject.id)
        .then()
            .body(matchesJsonSchemaInClasspath(projectJsonSchema));

        // the project is not found in webservice call for all projects
        assertThat(projectExistsInCollection(createdProject.id), equalTo(false));
    }

    private boolean projectExistsInCollection(String id) {
        List<ProjectDto> projects = Arrays.asList(
            given()
                .port(4000)
                .get("/api/manager/secure/project")
                .as(ProjectDto[].class));

        boolean foundProject = false;
        for (ProjectDto project: projects) {
            if (project.id.equals(id)) {
                foundProject = true;
                break;
            }
        }

        return foundProject;
    }
}
