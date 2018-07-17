package com.conveyal.datatools.manager.controllers.api;

import com.auth0.client.auth.AuthAPI;
import com.auth0.client.mgmt.ManagementAPI;
import com.auth0.exception.Auth0Exception;
import com.auth0.json.auth.TokenHolder;
import com.auth0.json.mgmt.users.User;
import com.auth0.net.AuthRequest;
import com.conveyal.datatools.manager.auth.Auth0UserProfile;
import com.conveyal.datatools.manager.models.*;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.conveyal.datatools.manager.utils.json.JsonManager;
import com.conveyal.datatools.manager.DataManager;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.*;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import java.io.*;
import java.net.URLEncoder;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.*;

import com.conveyal.datatools.manager.auth.Auth0Users;

import static com.conveyal.datatools.common.utils.SparkUtils.haltWithMessage;
import static com.conveyal.datatools.manager.auth.Auth0Users.getUserById;
import static org.apache.commons.lang.CharEncoding.UTF_8;
import static spark.Spark.*;

/**
 * Handles the HTTP endpoints related to CRUD operations for Auth0 users.
 */
public class UserController {

    public static String AUTH0_DOMAIN = DataManager.getConfigPropertyAsText("AUTH0_DOMAIN");
    private static String AUTH0_URL = "https://" + AUTH0_DOMAIN + "/api/v2/users";
    private static String AUTH0_CLIENT_ID = DataManager.getConfigPropertyAsText("AUTH0_CLIENT_ID");
    private static String AUTH0_SECRET = DataManager.getConfigPropertyAsText("AUTH0_SECRET");
//    private static String AUTH0_API_TOKEN = DataManager.getConfigPropertyAsText("AUTH0_TOKEN");
    private static final AuthAPI AUTH_API = new AuthAPI(AUTH0_DOMAIN, AUTH0_CLIENT_ID, AUTH0_SECRET);
    public static ManagementAPI mgmt;
    private static TokenHolder holder;
    private static final HttpClient client = HttpClientBuilder.create().build();
    private static Logger LOG = LoggerFactory.getLogger(UserController.class);
    private static ObjectMapper mapper = new ObjectMapper();
    public static JsonManager<Project> json =
            new JsonManager<>(Project.class, JsonViews.UserInterface.class);

    /**
     * HTTP endpoint to get a single Auth0 user for the application (by specified ID param). Note, this uses a different
     * Auth0 API (get user) than the other get methods (user search query).
     */
    private static String getUser(Request req, Response res) throws IOException {
        String userId = req.params("id");
        Auth0UserProfile user = req.attribute("user");
        if (!user.getUser_id().equals(userId) && !user.canAdministerApplication()) {
            // If the user ID does not match requesting user and user cannot administer application, do not permit the
            // the get request.
            haltWithMessage(401, "Must be application administrator to view user credentials.");
        }
        HttpGet request = new HttpGet(constructUserURL(userId));
//        mgmt = new ManagementAPI(AUTH0_DOMAIN, holder.getAccessToken());
        request.addHeader("Authorization", "Bearer " + getAuthToken());
        request.setHeader("Accept-Charset", UTF_8);

        HttpResponse response = client.execute(request);
        return EntityUtils.toString(response.getEntity());
    }

    private static User getUserInfo(Request req, Response res) throws IOException {
        User user = req.attribute("authUser");
        return user;
    }

    public static String getAuthToken () {
        String token;
        if (holder != null && holder.getExpiresIn() > 1000) {
            token = holder.getAccessToken();
            if (token != null) return token;
        }
        // If token not found, fetch new token.
        AuthRequest authRequest = AUTH_API.requestToken(String.format("https://%s/api/v2/", AUTH0_DOMAIN))
                .setScope("read:users read:user_idp_tokens");
//                .setScope("read:user_idp_tokens")
//                .setScope("openid email nickname");
        try {
            LOG.info("Fetching new Auth0 token");
            holder = authRequest.execute();
        } catch (Auth0Exception e) {
            e.printStackTrace();
            throw new IllegalStateException("Could not fetch Auth0 token");
        }
        LOG.info("New token expires in {}", holder.getExpiresIn());
        return holder.getAccessToken();
    }

    /**
     * HTTP endpoint to get all users for the application (using a filtered search on all users for the Auth0 tenant).
     */
    private static String getAllUsers(Request req, Response res) {
        // NOTE: permissions check for user occurs in the filterUserSearchQuery call (filters user list by organization
        // if applicable and halts request if user is not an application or organization admin).
        res.type("application/json");
        int page = Integer.parseInt(req.queryParams("page"));
        String queryString = filterUserSearchQuery(req);
        return Auth0Users.getAuth0Users(queryString, page);
    }

    /**
     * Filters a search query for users by the query string and the requesting user's permissions. For example, an
     * organization admin is only permitted to view the users assigned to that organization, whereas an application
     * admin can view all users for all organizations.
     */
    private static String filterUserSearchQuery(Request req) {
        Auth0UserProfile userProfile = req.attribute("user");
        String queryString = req.queryParams("queryString");
        if(queryString != null) queryString = "email:" + queryString + "*";

        if (userProfile.canAdministerApplication()) {
            // do not filter further based on permissions, proceed with search
            return queryString;
        } else if (userProfile.canAdministerOrganization()) {
            String organizationId = userProfile.getOrganizationId();
            // filter by organization_id
            if (queryString == null) {
                queryString = "app_metadata.datatools.organizations.organization_id:" + organizationId;
            } else {
                queryString += " AND app_metadata.datatools.organizations.organization_id:" + organizationId;
            }
            return queryString;
        } else {
            haltWithMessage(401, "Must be application or organization admin to view users");
            // Return statement cannot be reached due to halt.
            return null;
        }
    }

    /**
     * Gets the total count of users that match the filtered user search query.
     */
    private static int getUserCount(Request req, Response res) throws IOException {
        res.type("application/json");
        String queryString = filterUserSearchQuery(req);
        return Auth0Users.getAuth0UserCount(queryString);
    }

    /**
     * HTTP endpoint to create a "public user" that has no permissions to access projects in the application.
     *
     * Note, this passes a "blank" app_metadata object to the newly created user, so there is no risk of someone
     * injecting permissions somehow into the create user request.
     */
    private static String createPublicUser(Request req, Response res) throws IOException {
        // Construct JSON for new user object.
        JsonNode requestJSON = mapper.readTree(req.body());
        ObjectNode datatoolsObject = mapper.createObjectNode();
        datatoolsObject.set("permissions", mapper.createArrayNode());
        datatoolsObject.set("projects", mapper.createArrayNode());
        datatoolsObject.set("subscriptions", mapper.createArrayNode());
        datatoolsObject.put("client_id", AUTH0_CLIENT_ID);
        ObjectNode json = constructUserJSON(requestJSON.get("email").asText(), requestJSON.get("password").asText(), datatoolsObject);
        LOG.info("Creating user for {}", json.get("email").asText());
        // Construct HTTP request.
        HttpPost request = new HttpPost(AUTH0_URL);
        request.addHeader("Authorization", "Bearer " + getAuthToken());
        request.setHeader("Accept-Charset", UTF_8);
        request.setHeader("Content-Type", "application/json");
        HttpEntity entity = new ByteArrayEntity(json.toString().getBytes(UTF_8));
        request.setEntity(entity);
        // Execute create user request to Auth0.
        HttpResponse response = client.execute(request);
        int statusCode = response.getStatusLine().getStatusCode();
        if(statusCode >= 300) haltWithMessage(statusCode, response.toString());

        return EntityUtils.toString(response.getEntity());
    }

    /**
     * HTTP endpoint to create new Auth0 user for the application.
     *
     * FIXME: This endpoint fails if the user's email already exists in the Auth0 tenant.
     */
    private static String createUser(Request req, Response res) throws IOException {
        // Check permissions
        Auth0UserProfile user = req.attribute("user");
        if (!user.canAdministerApplication() && !user.canAdministerOrganization()) {
            // If the user cannot administer application or an organization, do not permit the
            // the get request.
            haltWithMessage(HttpStatus.SC_UNAUTHORIZED, "Must be application or organization administrator to create user.");
        }
        // Construct user JSON.
        JsonNode jsonNode = mapper.readTree(req.body());
        ObjectNode json = constructUserJSON(jsonNode.get("email").asText(), jsonNode.get("password").asText(), jsonNode.get("permissions"));
        LOG.info("Creating user: {}", json.get("email"));
        mapper.writerWithDefaultPrettyPrinter().writeValueAsString(json);
        // Construct HTTP request
        HttpPost request = new HttpPost(AUTH0_URL);
        request.addHeader("Authorization", "Bearer " + getAuthToken());
        request.setHeader("Accept-Charset", UTF_8);
        request.setHeader("Content-Type", "application/json");
        HttpEntity entity = new ByteArrayEntity(json.toString().getBytes(UTF_8));
        request.setEntity(entity);
        // Execute request to Auth0.
        HttpResponse response = client.execute(request);
        String result = EntityUtils.toString(response.getEntity());
        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode >= 300) haltWithMessage(statusCode, response.toString());
        return result;
    }

    /**
     * Helper method to construct an Auth0 user for use in a create user request.
     */
    private static ObjectNode constructUserJSON(String email, String password, JsonNode datatoolsObject) {
        ObjectNode userJSON = mapper.createObjectNode();
        ObjectNode appMetadata = mapper.createObjectNode();
        userJSON.put("connection", "Username-Password-Authentication");
        userJSON.put("email", email);
        userJSON.put("password", password);
        ArrayNode datatoolsList = mapper.createArrayNode();
        datatoolsList.add(datatoolsObject);
        appMetadata.set("datatools", datatoolsList);
        userJSON.set("app_metadata", appMetadata);
        return userJSON;
    }

    /**
     * Helper method to construct the Auth0 URL for the provided user ID.
     */
    private static String constructUserURL (String userId) {
        try {
            return String.join("/", AUTH0_URL, URLEncoder.encode(userId, UTF_8));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            throw new IllegalStateException("User ID cannot be URL-encoded.");
        }
    }

    /**
     * Update the app_metadata object for the specified Auth0 user.
     */
    private static String updateUser(Request req, Response res) throws IOException {
        // Check permissions
        Auth0UserProfile user = req.attribute("user");
        if (!user.canAdministerApplication() && !user.canAdministerOrganization()) {
            // If the user cannot administer application or an organization, do not permit the
            // the get request.
            haltWithMessage(HttpStatus.SC_UNAUTHORIZED, "Must be application or organization administrator to update user.");
        }
        String userId = req.params("id");
        Auth0UserProfile userToUpdate = getUserById(userId);
        if (userToUpdate == null) {
            // Halt if the user ID provided is not valid (i.e., user does not exist for Auth0 tenant).
            haltWithMessage(400, "User ID not valid.");
            return null;
        }

        LOG.info("Updating user {}", userToUpdate.getEmail());

        HttpPatch request = new HttpPatch(constructUserURL(userId));

        request.addHeader("Authorization", "Bearer " + getAuthToken());
        request.setHeader("Accept-Charset", UTF_8);
        request.setHeader("Content-Type", "application/json");

        JsonNode jsonNode = mapper.readTree(req.body());
        JsonNode data = jsonNode.get("data");
        ObjectNode json = mapper.createObjectNode();
        ObjectNode datatools = mapper.createObjectNode();
        datatools.set("datatools", data);
        json.set("app_metadata", datatools);
        HttpEntity entity = new ByteArrayEntity(json.toString().getBytes(UTF_8));
        request.setEntity(entity);

        HttpResponse response = client.execute(request);
        return EntityUtils.toString(response.getEntity());
    }

    /**
     * Delete an Auth0 user by ID.
     */
    private static Object deleteUser(Request req, Response res) throws IOException {
        // Check permissions
        Auth0UserProfile user = req.attribute("user");
        if (!user.canAdministerApplication() && !user.canAdministerOrganization()) {
            // If the user cannot administer application or an organization, do not permit the
            // the get request.
            haltWithMessage(HttpStatus.SC_UNAUTHORIZED, "Must be application or organization administrator to delete user.");
        }
        String url = constructUserURL(req.params("id"));
        HttpDelete request = new HttpDelete(url);
        request.addHeader("Authorization", "Bearer " + getAuthToken());
        request.setHeader("Accept-Charset", UTF_8);

        HttpResponse response = client.execute(request);
        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode >= 300) haltWithMessage(statusCode, response.getStatusLine().getReasonPhrase());

        return true;
    }

    private static List<Activity> getRecentActivity(Request req, Response res) {
        Auth0UserProfile userProfile = req.attribute("user");

        /* TODO: Allow custom from/to range
        String fromStr = req.queryParams("from");
        String toStr = req.queryParams("to"); */

        // Default range: past 7 days
        ZonedDateTime from = ZonedDateTime.now(ZoneOffset.UTC).minusDays(7);
        ZonedDateTime to = ZonedDateTime.now(ZoneOffset.UTC);

        List<Activity> activityList = new ArrayList<>();
        Auth0UserProfile.DatatoolsInfo datatools = userProfile.getApp_metadata().getDatatoolsInfo();
        if (datatools == null) {
            // NOTE: this condition will also occur if DISABLE_AUTH is set to true
            haltWithMessage(403, "User does not have permission to access to this application");
        }

        Auth0UserProfile.Subscription[] subscriptions = datatools.getSubscriptions();
        if (subscriptions == null) return activityList;

        /* NOTE: as of May-08-2018 we decided to limit subscriptions to two types:
         * 'feed-updated' and 'project-updated'. Comment subscriptions are now always
         * assumed if the containing 'feed-updated' subscription is active
         */
        for (Auth0UserProfile.Subscription sub : subscriptions) {
            switch (sub.getType()) {
                case "feed-updated":
                    for (String targetId : sub.getTarget()) {
                        FeedSource fs = Persistence.feedSources.getById(targetId);
                        if (fs == null) continue;

                        // FeedSource comments
                        for (Note note : fs.retrieveNotes()) {
                            ZonedDateTime datePosted = toZonedDateTime(note.date);
                            if (datePosted.isBefore(from) || datePosted.isAfter(to)) continue;
                            activityList.add(new FeedSourceCommentActivity(note, fs));
                        }

                        // Iterate through this Feed's FeedVersions
                        for(FeedVersion version : fs.retrieveFeedVersions()) {
                            // FeedVersion creation event
                            ZonedDateTime dateCreated = toZonedDateTime(fs.dateCreated);
                            if (dateCreated.isAfter(from) && dateCreated.isBefore(to)) {
                                activityList.add(new FeedVersionCreationActivity(version, fs));
                            }

                            // FeedVersion comments
                            for (Note note : version.retrieveNotes()) {
                                ZonedDateTime datePosted = toZonedDateTime(note.date);
                                if (datePosted.isBefore(from) || datePosted.isAfter(to)) continue;
                                activityList.add(new FeedVersionCommentActivity(note, fs, version));
                            }
                        }
                    }
                    break;

                case "project-updated":
                    // Iterate through Project IDs, skipping any that don't resolve to actual projects
                    for (String targetId : sub.getTarget()) {
                        Project project = Persistence.projects.getById(targetId);
                        if (project == null) continue;

                        // Iterate through Project's FeedSources, creating "Feed created" items as needed
                        for (FeedSource fs : project.retrieveProjectFeedSources()) {
                            ZonedDateTime dateCreated = toZonedDateTime(fs.dateCreated);
                            if (dateCreated.isBefore(from) || dateCreated.isAfter(to)) continue;
                            activityList.add(new FeedSourceCreationActivity(fs, project));
                        }
                    }
                    break;
            }
        }

        return activityList;
    }

    private static ZonedDateTime toZonedDateTime (Date date) {
        return ZonedDateTime.ofInstant(date.toInstant(), ZoneOffset.UTC);
    }

    static abstract class Activity implements Serializable {
        private static final long serialVersionUID = 1L;
        public String type;
        public String userId;
        public String userName;
        public Date date;
    }

    static class FeedSourceCreationActivity extends Activity {
        private static final long serialVersionUID = 1L;
        public String feedSourceId;
        public String feedSourceName;
        public String projectId;
        public String projectName;

        public FeedSourceCreationActivity(FeedSource fs, Project proj) {
            this.type = "feed-created";
            this.date = fs.dateCreated;
            this.userId = fs.userId;
            this.userName = fs.userEmail;
            this.feedSourceId = fs.id;
            this.feedSourceName = fs.name;
            this.projectId = proj.id;
            this.projectName = proj.name;
        }
    }

    static class FeedVersionCreationActivity extends Activity {
        private static final long serialVersionUID = 1L;
        public Integer feedVersionIndex;
        public String feedVersionName;
        public String feedSourceId;
        public String feedSourceName;

        public FeedVersionCreationActivity(FeedVersion version, FeedSource fs) {
            this.type = "version-created";
            this.date = version.dateCreated;
            this.userId = version.userId;
            this.userName = version.userEmail;
            this.feedVersionIndex = version.version;
            this.feedVersionName = version.name;
            this.feedSourceId = fs.id;
            this.feedSourceName = fs.name;
        }
    }

    static abstract class CommentActivity extends Activity {
        private static final long serialVersionUID = 1L;
        public String body;

        public CommentActivity (Note note) {
            this.date = note.date;
            this.userId = note.userId;
            this.userName = note.userEmail;
            this.body = note.body;
        }
    }

    static class FeedSourceCommentActivity extends CommentActivity {
        private static final long serialVersionUID = 1L;
        public String feedSourceId;
        public String feedSourceName;

        public FeedSourceCommentActivity(Note note, FeedSource feedSource) {
            super(note);
            this.type = "feed-commented-on";
            this.feedSourceId = feedSource.id;
            this.feedSourceName = feedSource.name;
        }
    }

    static class FeedVersionCommentActivity extends FeedSourceCommentActivity {
        private static final long serialVersionUID = 1L;
        public Integer feedVersionIndex;
        public String feedVersionName;

        public FeedVersionCommentActivity(Note note, FeedSource feedSource, FeedVersion version) {
            super(note, feedSource);
            this.type = "version-commented-on";
            this.feedVersionIndex = version.version;
            this.feedVersionName = version.name;
        }
    }

    public static void register (String apiPrefix) {
        get(apiPrefix + "secure/user/:id", UserController::getUser, json::write);
        get(apiPrefix + "secure/user/:id/recentactivity", UserController::getRecentActivity, json::write);
        get(apiPrefix + "secure/user", UserController::getAllUsers, json::write);
        get(apiPrefix + "secure/userinfo", UserController::getUserInfo, json::write);
        get(apiPrefix + "secure/usercount", UserController::getUserCount, json::write);
        post(apiPrefix + "secure/user", UserController::createUser, json::write);
        put(apiPrefix + "secure/user/:id", UserController::updateUser, json::write);
        delete(apiPrefix + "secure/user/:id", UserController::deleteUser, json::write);

        post(apiPrefix + "public/user", UserController::createPublicUser, json::write);
    }
}
