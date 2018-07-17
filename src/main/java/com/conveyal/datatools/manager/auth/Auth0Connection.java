package com.conveyal.datatools.manager.auth;

import com.auth0.client.mgmt.ManagementAPI;
import com.auth0.client.mgmt.filter.UserFilter;
import com.auth0.json.mgmt.users.User;
import com.conveyal.datatools.common.utils.SparkUtils;
import com.conveyal.datatools.manager.DataManager;
import com.conveyal.datatools.manager.controllers.api.UserController;
import com.conveyal.datatools.manager.models.FeedSource;
import com.conveyal.datatools.manager.persistence.Persistence;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Arrays;

import static com.conveyal.datatools.common.utils.SparkUtils.haltWithMessage;
import static com.conveyal.datatools.manager.DataManager.getConfigPropertyAsText;
import static com.conveyal.datatools.manager.controllers.api.UserController.AUTH0_DOMAIN;
import static com.conveyal.datatools.manager.controllers.api.UserController.getAuthToken;
import static org.apache.commons.lang.CharEncoding.UTF_8;
import static spark.Spark.halt;

/**
 * Created by demory on 3/22/16.
 */

public class Auth0Connection {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Logger LOG = LoggerFactory.getLogger(Auth0Connection.class);
    private static final String BASE_URL = getConfigPropertyAsText("application.public_url");
    private static final int DEFAULT_LINES_TO_PRINT = 10;
    private static final HttpClient client = HttpClients.createDefault();

    /**
     * Check API request for user token and assign as the "user" attribute on the incoming request object for use in
     * downstream controllers.
     * @param req Spark request object
     */
    public static void checkUser(Request req) {
        // If in a development environment, assign a mock profile to request attribute
        if (authDisabled()) {
            req.attribute("user", new Auth0UserProfile("mock@example.com", "user_id:string"));
            return;
        }
        String token = getToken(req);

        if(token == null) {
            haltWithMessage(401, "Could not find authorization token");
        }
        try {
            // Construct HTTP request.
            HttpPost request = new HttpPost("https://" + AUTH0_DOMAIN + "/userinfo");
            request.addHeader("Authorization", "Bearer " + token);
            request.setHeader("Accept-Charset", UTF_8);
            request.setHeader("Content-Type", "application/json");
            //Execute and get the response.
            HttpResponse response = client.execute(request);
            int statusCode = response.getStatusLine().getStatusCode();
            HttpEntity entity = response.getEntity();
            String responseString = EntityUtils.toString(entity);
            if (statusCode >= 300) {
                LOG.error("Error response from Auth0: {}", responseString);
                throw new IllegalStateException("Could not verify user");
            }
            // Assign Auth0 user profile (and raw JSON NODE) to Spark request.
            Auth0UserProfile profile = MAPPER.readValue(responseString, Auth0UserProfile.class);
            String authToken = getAuthToken();
            UserController.mgmt = new ManagementAPI(AUTH0_DOMAIN, authToken);
            JsonNode jsonUser = MAPPER.readTree(responseString);
            User user = UserController.mgmt.users().get(jsonUser.get("sub").asText(), new UserFilter()).execute();
            req.attribute("authUser", user);
            req.attribute("user", profile);
            req.attribute("rawUser", MAPPER.readTree(responseString));
        }
        catch(Exception e) {
            LOG.warn("Could not verify user", e);
            haltWithMessage(401, "Could not verify user");
        }
    }

    private static String getToken(Request req) {
        String token = null;

        final String authorizationHeader = req.headers("Authorization");
        if (authorizationHeader == null) return null;

        // check format (Authorization: Bearer [token])
        String[] parts = authorizationHeader.split(" ");
        if (parts.length != 2) return null;

        String scheme = parts[0];
        String credentials = parts[1];

        if (scheme.equals("Bearer")) token = credentials;
        if ("null".equals(token)) return null;
        return token;
    }

    /**
     * Check that the user has edit privileges for the feed ID specified.
     * FIXME: Needs an update for SQL editor.
     */
    public static void checkEditPrivileges(Request request) {
        // If in a development environment, assign a mock profile to request attribute
        if (authDisabled()) {
            request.attribute("user", new Auth0UserProfile("mock@example.com", "user_id:string"));
            return;
        }
        Auth0UserProfile userProfile = request.attribute("user");
        String feedId = request.queryParams("feedId");
        if (feedId == null) {
            String[] parts = request.pathInfo().split("/");
            feedId = parts[parts.length - 1];
        }
        FeedSource feedSource = feedId != null ? Persistence.feedSources.getById(feedId) : null;
        if (feedSource == null) {
            LOG.warn("feedId {} not found", feedId);
            halt(400, SparkUtils.formatJSON("Must provide valid feedId parameter", 400));
        }

        if (!request.requestMethod().equals("GET")) {
            if (!userProfile.canEditGTFS(feedSource.organizationId(), feedSource.projectId, feedSource.id)) {
                LOG.warn("User {} cannot edit GTFS for {}", userProfile.email, feedId);
                halt(403, SparkUtils.formatJSON("User does not have permission to edit GTFS for feedId", 403));
            }
        }
    }

    /**
     * Check whether authentication has been disabled via the DISABLE_AUTH config variable.
     */
    public static boolean authDisabled() {
        return DataManager.hasConfigProperty("DISABLE_AUTH") && "true".equals(getConfigPropertyAsText("DISABLE_AUTH"));
    }

    /**
     * Log Spark requests.
     */
    public static void logRequest(Request request, Response response) {
        logRequestOrResponse(true, request, response);
    }

    /**
     * Log Spark responses.
     */
    public static void logResponse(Request request, Response response) {
        logRequestOrResponse(false, request, response);
    }

    /**
     * Log request/response.  Pretty print JSON if the content-type is JSON.
     */
    public static void logRequestOrResponse(boolean logRequest, Request request, Response response) {
        Auth0UserProfile userProfile = request.attribute("user");
        String userEmail = userProfile != null ? userProfile.email : "no-auth";
        HttpServletResponse raw = response.raw();
        // NOTE: Do not attempt to read the body into a string until it has been determined that the content-type is
        // JSON.
        String bodyString = "";
        try {
            String contentType = logRequest ? request.contentType() : raw.getHeader("content-type");
            if ("application/json".equals(contentType)) {
                bodyString = logRequest ? request.body() : response.body();
                if (bodyString == null) return;
                // Pretty print JSON if ContentType is JSON and body is not empty
                JsonNode jsonNode = MAPPER.readTree(bodyString);
                // Add new line for legibility when printing
                bodyString = "\n" + MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(jsonNode);
            }
        } catch (IOException e) {
            LOG.warn("Could not parse JSON", e);
            bodyString = "\nBad JSON:\n" + bodyString;
        }

        String queryString = request.queryParams().size() > 0 ? "?" + request.queryString() : "";
        LOG.info(
            "{} {} {}: {}{}{}{}",
            logRequest ? "req" : String.format("res (%s)", raw.getStatus()),
            userEmail,
            request.requestMethod(),
            BASE_URL,
            request.pathInfo(),
            queryString,
            trimLines(bodyString)
        );
    }

    private static String trimLines(String str) {
        if (str == null) return "";
        String[] lines = str.split("\n");
        if (lines.length <= DEFAULT_LINES_TO_PRINT) return str;
        return String.format(
            "%s \n...and %d more lines",
            String.join("\n", Arrays.copyOfRange(lines, 0, DEFAULT_LINES_TO_PRINT - 1)),
            lines.length - DEFAULT_LINES_TO_PRINT
        );
    }

    /**
     * TODO: Check that user has access to query namespace provided in GraphQL query (see https://github.com/catalogueglobal/datatools-server/issues/94).
     */
    public static void checkGTFSPrivileges(Request request) {
        Auth0UserProfile userProfile = request.attribute("user");
        String feedId = request.queryParams("feedId");
        if (feedId == null) {
            String[] parts = request.pathInfo().split("/");
            feedId = parts[parts.length - 1];
        }
        FeedSource feedSource = feedId != null ? Persistence.feedSources.getById(feedId) : null;
        if (feedSource == null) {
            LOG.warn("feedId {} not found", feedId);
            halt(400, SparkUtils.formatJSON("Must provide valid feedId parameter", 400));
        }

        if (!request.requestMethod().equals("GET")) {
            if (!userProfile.canEditGTFS(feedSource.organizationId(), feedSource.projectId, feedSource.id)) {
                LOG.warn("User {} cannot edit GTFS for {}", userProfile.email, feedId);
                halt(403, SparkUtils.formatJSON("User does not have permission to edit GTFS for feedId", 403));
            }
        }
    }
}
