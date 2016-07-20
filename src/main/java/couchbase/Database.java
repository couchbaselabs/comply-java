package couchbase;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.*;
import com.couchbase.client.java.query.consistency.ScanConsistency;
import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.crypto.bcrypt.BCrypt;

import java.util.*;

public class Database {

    private Database() { }

    /*
     * ###################################################
     * ################# User Functions ##################
     * ###################################################
     */


    /*
     * Get a single user document based on its id information. The result will be an array of objects.
     */
    public static List<Map<String, Object>> getUserById(final Bucket bucket, String userId) {
        String queryStr = "SELECT _id, _type, name, address, company, username, phone, `password` " +
                       "FROM `" + bucket.name() + "` AS users " +
                       "WHERE _type = 'User' AND META(users).id = $1";
        ParameterizedN1qlQuery query = ParameterizedN1qlQuery.parameterized(queryStr, JsonArray.create().add(userId));
        N1qlQueryResult queryResult = bucket.query(query);
        return extractResultOrThrow(queryResult);
    }

    /*
     * Get all documents that have a property called _type.  The result will be an array of objects.
     */
    public static List<Map<String, Object>> getUsers(final Bucket bucket) {
        String queryStr = "SELECT _id, _type, name, address, company, username, phone, `password` FROM `" + bucket.name() + "` WHERE _type = 'User'";
        N1qlQueryResult queryResult = bucket.query(N1qlQuery.simple(queryStr));
        return extractResultOrThrow(queryResult);
    }

    /*
     * Attempt to get a document by the username which also represents a document key.  If the document is found, use Bcrypt to
     * validate the password.  If everything succeeds, return the user document itself.  The password returned will be Bcrypted, not plain text.
     */
    public static ResponseEntity<String> login(final Bucket bucket, String username, String password) {
        JsonObject response;
        HttpStatus responseStatus;
        JsonDocument user = bucket.get(username);
        if(user == null) {
            response = JsonObject.create().put("error", 401).put("message", "The username provided does not exist or was not correct");
            responseStatus = HttpStatus.UNAUTHORIZED;
        } else {
            JsonObject jsonUser = user.content();
            if(BCrypt.checkpw(password, jsonUser.getString("password"))) {
                response = jsonUser;
                responseStatus = HttpStatus.OK;
            } else {
                response = JsonObject.create().put("error", 401).put("message", "The password provided is not correct");
                responseStatus = HttpStatus.UNAUTHORIZED;
            }
        }
        return new ResponseEntity<String>(response.toString(), responseStatus);
    }

    /*
     * Attempt to create a new user document based on the data that was passed through.  To keep things simple, the document id will be
     * the username.  The password will be Bcrypted and stored for security. The insert will fail if the document id already exists.
     */
    public static ResponseEntity<String> createUser(final Bucket bucket, JsonObject data, String username, String password) {
        JsonObject response;
        HttpStatus responseStatus;
        JsonDocument document = JsonDocument.create(username, data.put("_id", username).put("_type", "User").put("password", BCrypt.hashpw(password, BCrypt.gensalt())));
        try {
            bucket.insert(document);
            response = document.content();
            responseStatus = HttpStatus.OK;
        } catch (Exception e) {
            response = JsonObject.create().put("error", 409).put("message", e.getMessage());
            responseStatus = HttpStatus.CONFLICT;
        }
        return new ResponseEntity<String>(response.toString(), responseStatus);
    }


    /*
     * ###################################################
     * ############### Company Endpoints #################
     * ###################################################
     */


    /*
     * Get a company from the database based on its document _type and particular id.  The data returned is an array of objects.
     */
    public static List<Map<String, Object>> getCompanyById(final Bucket bucket, String companyId) {
        String queryStr = "SELECT _id, _type, name, address, phone, website " +
                       "FROM `" + bucket.name() + "` AS companies " +
                       "WHERE _type = 'Company' AND META(companies).id = $1";
        ParameterizedN1qlQuery query = ParameterizedN1qlQuery.parameterized(queryStr, JsonArray.create().add(companyId));
        N1qlQueryResult queryResult = bucket.query(query);
        return extractResultOrThrow(queryResult);
    }

    /*
     * Get all companies from the database based on the document _type
     */
    public static List<Map<String, Object>> getCompanies(final Bucket bucket) {
        String queryStr = "SELECT _id, _type, name, address, phone, website FROM `" + bucket.name() + "` WHERE _type = 'Company'";
        N1qlQueryResult queryResult = bucket.query(N1qlQuery.simple(queryStr));
        return extractResultOrThrow(queryResult);
    }

    /*
     * Create a company and use the website as the document id
     */
    public static ResponseEntity<String> createCompany(final Bucket bucket, JsonObject data) {
        JsonObject response;
        HttpStatus responseStatus;
        JsonDocument document = JsonDocument.create(data.getString("website"), data.put("_id", data.getString("website")).put("_type", "Company"));
        try {
            bucket.insert(document);
            response = document.content();
            responseStatus = HttpStatus.OK;
        } catch (Exception e) {
            response = JsonObject.create().put("error", 409).put("message", e.getMessage());
            responseStatus = HttpStatus.CONFLICT;
        }
        return new ResponseEntity<String>(response.toString(), responseStatus);
    }


    /*
     * ###################################################
     * ################ Project Endpoints ################
     * ###################################################
     */

    /*
     * Get a particular project by the project id.  With the project document includes an expanded owner property, rather than just the owner id.  It also
     * includes expanded users rather than an array of user ids, and it includes expanded tasks rather than a list of tasks.  The result is an array of objects.
     */
    public static List<Map<String, Object>> getProjectById(final Bucket bucket, String projectId) {
        String queryStr = "SELECT _id, createdON, description,name, " +
                "(SELECT _id, _type, active, address, company, createdON, name, `password`, phone " +
                "FROM `" + bucket.name() + "` USE KEYS c.owner)[0] as owner, (SELECT _id, _type, active, " +
                "address, company, createdON, name, `password`, phone FROM `" + bucket.name() + "` USE KEYS " +
                "c.users) AS users, (SELECT _id, name, description, owner, assignedTo, Users, " +
                "history, permalink FROM `" + bucket.name() + "` USE KEYS c.tasks) as tasks, permalink FROM " +
                " `" + bucket.name() + "` c WHERE c._id=$1";
        ParameterizedN1qlQuery query = ParameterizedN1qlQuery.parameterized(queryStr, JsonArray.create().add(projectId));
        N1qlQueryResult queryResult = bucket.query(query);
        return extractResultOrThrow(queryResult);
    }

    /*
     * Get all projects from the database for a particular owner id
     */
    public static List<Map<String, Object>> getProjectsByOwnerId(final Bucket bucket, String ownerId) {
        String queryStr = "SELECT _id, _type, owner, users, tasks, description, name FROM `" + bucket.name() + "` WHERE _type = 'Project' and owner = $1";
        ParameterizedN1qlQuery query = ParameterizedN1qlQuery.parameterized(queryStr, JsonArray.create().add(ownerId));
        N1qlQueryResult queryResult = bucket.query(query);
        return extractResultOrThrow(queryResult);
    }

    /*
     * Get all projects based on the document _type property
     */
    public static List<Map<String, Object>> getProjects(final Bucket bucket) {
        String queryStr = "SELECT _id, _type, name, description, tasks, users, owner FROM `" + bucket.name() + "` AS projects WHERE _type = 'Project'";
        N1qlQueryResult queryResult = bucket.query(N1qlQuery.simple(queryStr));
        return extractResultOrThrow(queryResult);
    }

    /*
     * Get all projects for a user that has access permissions to them based on being in the users array
     */
    public static List<Map<String, Object>> getOtherProjectsByUserId(final Bucket bucket, String userId) {
        String queryStr = "SELECT _id, _type, name, description, tasks, users, owner FROM `" + bucket.name() + "` WHERE _type = 'Project' AND ANY x IN users SATISFIES x = $1 END";
        ParameterizedN1qlQuery query = ParameterizedN1qlQuery.parameterized(queryStr, JsonArray.create().add(userId));
        N1qlQueryResult queryResult = bucket.query(query);
        return extractResultOrThrow(queryResult);
    }

    /*
     * Create a new project in the database
     */
    public static ResponseEntity<String> createProject(final Bucket bucket, JsonObject data) {
        JsonObject response;
        HttpStatus responseStatus;
        String documentId = UUID.randomUUID().toString();
        JsonArray users = data.getArray("users");
        users.add(data.getString("owner"));
        JsonDocument document = JsonDocument.create(documentId, data.put("_id", documentId).put("_type", "Project").put("users", users).put("createdON", (new Date()).toString()));
        try {
            bucket.upsert(document);
            response = document.content();
            responseStatus = HttpStatus.OK;
        } catch (Exception e) {
            response = JsonObject.create().put("error", 409).put("message", e.getMessage());
            responseStatus = HttpStatus.INTERNAL_SERVER_ERROR;
        }
        return new ResponseEntity<String>(response.toString(), responseStatus);
    }

    /*
     * Add a user to a particular project based on the existing user id and project id
     */
    public static ResponseEntity<String> projectAddUser(final Bucket bucket, JsonObject data) {
        JsonObject response;
        HttpStatus responseStatus;
        JsonDocument user = bucket.get(data.getString("username"));
        if(user == null) {
            response = JsonObject.create().put("error", 400).put("message", "The user does not exist");
            responseStatus = HttpStatus.BAD_REQUEST;
        }
        JsonDocument project = bucket.get(data.getString("projectId"));
        if(project == null) {
            response = JsonObject.create().put("error", 400).put("message", "The project does not exist");
            responseStatus = HttpStatus.BAD_REQUEST;
        }
        JsonObject jsonProject = project.content();
        JsonArray projectUsers = jsonProject.getArray("users");
        if(!projectUsers.toString().contains(data.getString("username"))) {
            projectUsers.add(data.getString("username"));
        }
        jsonProject.put("users", projectUsers);
        project = JsonDocument.create(jsonProject.getString("_id"), jsonProject);
        try {
            bucket.upsert(project);
            response = user.content();
            responseStatus = HttpStatus.OK;
        } catch (Exception e) {
            response = JsonObject.create().put("error", 409).put("message", e.getMessage());
            responseStatus = HttpStatus.INTERNAL_SERVER_ERROR;
        }
        return new ResponseEntity<String>(response.toString(), responseStatus);
    }


    /*
     * ###################################################
     * ################## Task Endpoints #################
     * ###################################################
     */


    /*
     * Get all tasks assigned to a particular user id from the database.  The assigned to information is expanded instead of providing
     * just a user id.  The owner information is expanded instead of showing just the user id. The users array is expanded instead of
     * showing a list of user ids.  An array of objects is returned.
     */
    public static List<Map<String, Object>> getTasksAssignedToUserId(final Bucket bucket, String userId) {
        String queryStr = "SELECT _id, (SELECT _id, _type, active," +
                "address, company, createdON, name, `password`, phone FROM `" + bucket.name() + "` USE KEYS " +
                "c.assignedTo)[0] AS assignedTo, createdON, description, history, name," +
                "(SELECT _id, _type, active, address, company, createdON, name, `password`, phone " +
                "FROM `" + bucket.name() + "` USE KEYS c.owner)[0] as owner, (SELECT _id, _type, active, address," +
                "company, createdON, name, `password`, phone FROM `" + bucket.name() + "` USE KEYS c.users) AS " +
                "users, permalink FROM `" + bucket.name() + "` c WHERE c.assignedTo = $1";
        ParameterizedN1qlQuery query = ParameterizedN1qlQuery.parameterized(queryStr, JsonArray.create().add(userId));
        N1qlQueryResult queryResult = bucket.query(query);
        return extractResultOrThrow(queryResult);
    }

    /*
     * Get a task from the database based on its task id.  Response includes the parent project id in which the task is
     * associated with, expanded assigned user information, expanded owner information, and expanded information for any user
     * that is attached to the task.  Returns an array of objects.
     */
    public static List<Map<String, Object>> getTaskById(final Bucket bucket, String taskId) {
        String queryStr = "SELECT (p._id) AS projectId, (SELECT _id, " +
                "(SELECT _id, _type, active, address, company, createdON, name, `password`, phone " +
                "FROM `" + bucket.name() + "` USE KEYS c.assignedTo)[0] AS assignedTo, createdON, " +
                "description, (select t.log, t.createdAt, (SELECT _id, _type, active, address, " +
                "company, createdON, name, `password`, phone FROM `" + bucket.name() + "` USE KEYS t.`user`)[0] " +
                "AS `user` FROM `" + bucket.name() + "` r UNNEST r.history t WHERE r._id = $1) AS history,name, " +
                "(SELECT _id, _type, active, address, company, createdON, name, `password`, phone " +
                "FROM `" + bucket.name() + "` USE KEYS c.owner)[0] as owner, (SELECT _id, _type, active, address, " +
                "company, createdON, name, `password`, phone FROM `" + bucket.name() + "` USE KEYS c.users) " +
                "AS users, permalink FROM `" + bucket.name() + "` c WHERE c._id= $1)[0] AS task FROM `" + bucket.name() + "` " +
                "p WHERE ANY x IN tasks SATISFIES x = $1 END ";
        ParameterizedN1qlQuery query = ParameterizedN1qlQuery.parameterized(queryStr, JsonArray.create().add(taskId));
        N1qlQueryResult queryResult = bucket.query(query);
        return extractResultOrThrow(queryResult);
    }

    /*
     * Create a task for an existing project assigned to an existing user
     */
    public static List<Map<String, Object>> createTask(final Bucket bucket, String projectId, JsonObject data) {
        JsonObject response = null;
        HttpStatus responseStatus = null;
        String taskId = UUID.randomUUID().toString();
        JsonArray users = data.getArray("users");
        users.add(data.getString("owner"));
        JsonDocument document = JsonDocument.create(taskId, data.put("_id", taskId).put("_type", "Task").put("users", users).put("createdON", (new Date()).toString()));

        try {
            bucket.upsert(document);
        } catch (Exception e) {
            response = JsonObject.create().put("error", 409).put("message", e.getMessage());
            responseStatus = HttpStatus.INTERNAL_SERVER_ERROR;
        }

        JsonDocument project = bucket.get(projectId);
        if(project == null) {
            response = JsonObject.create().put("error", 400).put("message", "The project id does not exist");
            responseStatus = HttpStatus.BAD_REQUEST;
        }
        JsonObject jsonProject = project.content();
        JsonArray jsonProjectTasks = jsonProject.getArray("tasks");
        jsonProjectTasks.add(taskId);
        jsonProject.put("tasks", jsonProjectTasks);
        project = JsonDocument.create(jsonProject.getString("_id"), jsonProject);
        try {
            bucket.upsert(project);
        } catch (Exception e) {
            response = JsonObject.create().put("error", 409).put("message", e.getMessage());
            responseStatus = HttpStatus.INTERNAL_SERVER_ERROR;
        }

        if(response != null && responseStatus != null) {
            List<Map<String, Object>> responseMap = new ArrayList<Map<String, Object>>();
            responseMap.add(response.toMap());
            return responseMap;
        }

        String queryStr = "SELECT c._id, c.createdON, c.name, c.description," +
                "(SELECT _id, _type, active, address, company, createdON, name, `password`, phone " +
                "FROM `" + bucket.name() + "` USE KEYS c.owner)[0] AS owner, c.status, (SELECT _id, _type," +
                "active, address, company, createdON, name, `password`, phone FROM `" + bucket.name() + "` " +
                " USE KEYS c.users) AS users, c.permalink FROM `" + bucket.name() + "` " +
                " c WHERE c._id = $1";
        N1qlParams params = N1qlParams.build().consistency(ScanConsistency.REQUEST_PLUS);
        ParameterizedN1qlQuery query = ParameterizedN1qlQuery.parameterized(queryStr, JsonArray.create().add(taskId), params);
        N1qlQueryResult queryResult = bucket.query(query);
        return extractResultOrThrow(queryResult);
    }

    /*
     * Add a new comment to the history of a task based on the task id and user id.  Returns the comment along with the
     * expanded user information that created it.  An array of objects are returned.
     */
    public static List<Map<String, Object>> taskAddHistory(final Bucket bucket, JsonObject data) {
        JsonObject response = null;
        HttpStatus responseStatus = null;
        JsonDocument task = bucket.get(data.getString("taskId"));
        if(task == null) {
            response = JsonObject.create().put("error", 400).put("message", "The task id does not exist");
            responseStatus = HttpStatus.BAD_REQUEST;
        }
        JsonObject jsonTask = task.content();
        JsonArray taskHistory = jsonTask.getArray("history");
        JsonObject currentTaskHistory = JsonObject.create().put("log", data.getString("log")).put("user", data.getString("userId")).put("createdAt", (new Date()).toString());
        taskHistory.add(currentTaskHistory);
        jsonTask.put("history", taskHistory);
        task = JsonDocument.create(data.getString("taskId"), jsonTask);
        try {
            bucket.upsert(task);
        } catch (Exception e) {
            response = JsonObject.create().put("error", 409).put("message", e.getMessage());
            responseStatus = HttpStatus.INTERNAL_SERVER_ERROR;
        }

        if(response != null && responseStatus != null) {
            List<Map<String, Object>> responseMap = new ArrayList<Map<String, Object>>();
            responseMap.add(response.toMap());
            return responseMap;
        }

        String queryStr = "SELECT ($1) AS log, (SELECT _id, _type, active," +
                "address, company, createdON, name, `password`, phone " +
                "FROM `" + bucket.name() + "` USE KEYS c._id)[0] AS `user`,($2) AS createdAt " +
                " FROM `" + bucket.name() + "` c WHERE c._id = $3 ";
        N1qlParams params = N1qlParams.build().consistency(ScanConsistency.REQUEST_PLUS);
        ParameterizedN1qlQuery query = ParameterizedN1qlQuery.parameterized(queryStr, JsonArray.create().add(data.getString("log")).add(currentTaskHistory.getString("createdAt")).add(data.getString("userId")), params);
        N1qlQueryResult queryResult = bucket.query(query);
        return extractResultOrThrow(queryResult);
    }

    /*
     * Assign a particular existing user to an existing task
     */
    public static ResponseEntity<String> taskAssignUser(final Bucket bucket, JsonObject data) {
        JsonObject response;
        HttpStatus responseStatus;
        JsonDocument user = bucket.get(data.getString("userId"));
        if(user == null) {
            response = JsonObject.create().put("error", 400).put("message", "The user id does not exist");
            responseStatus = HttpStatus.BAD_REQUEST;
        }
        JsonDocument task = bucket.get(data.getString("taskId"));
        if(task == null) {
            response = JsonObject.create().put("error", 400).put("message", "The task id does not exist");
            responseStatus = HttpStatus.BAD_REQUEST;
        }
        JsonObject jsonTask = task.content();
        jsonTask.put("assignedTo", data.getString("userId"));
        task = JsonDocument.create(jsonTask.getString("_id"), jsonTask);
        try {
            bucket.upsert(task);
            response = user.content();
            responseStatus = HttpStatus.OK;
        } catch (Exception e) {
            response = JsonObject.create().put("error", 409).put("message", e.getMessage());
            responseStatus = HttpStatus.INTERNAL_SERVER_ERROR;
        }
        return new ResponseEntity<String>(response.toString(), responseStatus);
    }

    /*
     * Add a user to an existing task, but not make it the assigned user
     */
    public static ResponseEntity<String> taskAddUser(final Bucket bucket, JsonObject data) {
        JsonObject response;
        HttpStatus responseStatus;
        JsonDocument user = bucket.get(data.getString("username"));
        if(user == null) {
            response = JsonObject.create().put("error", 400).put("message", "The user id does not exist");
            responseStatus = HttpStatus.BAD_REQUEST;
        }
        JsonDocument task = bucket.get(data.getString("taskId"));
        if(task == null) {
            response = JsonObject.create().put("error", 400).put("message", "The task id does not exist");
            responseStatus = HttpStatus.BAD_REQUEST;
        }
        JsonObject jsonTask = task.content();
        JsonArray taskUsers = jsonTask.getArray("users");
        if(!taskUsers.toString().contains(data.getString("username"))) {
            taskUsers.add(data.getString("username"));
        }
        jsonTask.put("users", taskUsers);
        task = JsonDocument.create(jsonTask.getString("_id"), jsonTask);
        try {
            bucket.upsert(task);
            response = user.content();
            responseStatus = HttpStatus.OK;
        } catch (Exception e) {
            response = JsonObject.create().put("error", 409).put("message", e.getMessage());
            responseStatus = HttpStatus.INTERNAL_SERVER_ERROR;
        }
        return new ResponseEntity<String>(response.toString(), responseStatus);
    }


    /*
     * Convert query results into a more friendly List object
     */
    private static List<Map<String, Object>> extractResultOrThrow(N1qlQueryResult result) {
        if (!result.finalSuccess()) {
            throw new DataRetrievalFailureException("Query error: " + result.errors());
        }
        List<Map<String, Object>> content = new ArrayList<Map<String, Object>>();
        for (N1qlQueryRow row : result) {
            content.add(row.value().toMap());
        }
        return content;
    }

}
