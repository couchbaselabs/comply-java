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

import java.util.*;

public class Database {

    private Database() { }



    public static List<Map<String, Object>> getUserById(final Bucket bucket, String userId) {
        String queryStr = "SELECT _id, _type, name, address, company, email, phone, `password` " +
                       "FROM `" + bucket.name() + "` AS users " +
                       "WHERE META(users).id = $1";
        ParameterizedN1qlQuery query = ParameterizedN1qlQuery.parameterized(queryStr, JsonArray.create().add(userId));
        N1qlQueryResult queryResult = bucket.query(query);
        return extractResultOrThrow(queryResult);
    }

    public static List<Map<String, Object>> getUsers(final Bucket bucket) {
        String queryStr = "SELECT _id, _type, name, address, company, email, phone, `password` FROM `" + bucket.name() + "` WHERE _type = 'User'";
        N1qlQueryResult queryResult = bucket.query(N1qlQuery.simple(queryStr));
        return extractResultOrThrow(queryResult);
    }

    public static ResponseEntity<String> login(final Bucket bucket, String email, String password) {
        /*String queryStr = "SELECT _id, active, address, company, createdON, email, name, `password`, phone " +
                       "FROM `" + bucket.name() + "` AS users " +
                       "WHERE _type = 'User' AND email = $1";
        ParameterizedN1qlQuery query = ParameterizedN1qlQuery.parameterized(queryStr, JsonArray.create().add(email));
        N1qlQueryResult queryResult = bucket.query(query);*/
        JsonDocument document = bucket.get(email);
        return new ResponseEntity<String>(document.content().toString(), HttpStatus.OK);
        //return extractResultOrThrow(queryResult);
    }

    public static ResponseEntity<String> createUser(final Bucket bucket, JsonObject data) {
        JsonDocument document = JsonDocument.create(data.getString("email"), data.put("_id", data.getString("email")).put("_type", "User"));
        bucket.upsert(document);
        JsonObject responseData = JsonObject.create()
                .put("success", true)
                .put("data", data);
        return new ResponseEntity<String>(responseData.toString(), HttpStatus.OK);
        //bucket.upsert(data.getString("_id"), data);
        /*String queryStr = "UPSERT INTO `" + bucket.name() + "` (KEY, VALUE) VALUES " +
                "($1, {'_type': 'User', '_id': $2, 'createdON': $3, 'active': true})";
        ParameterizedN1qlQuery query = ParameterizedN1qlQuery.parameterized(queryStr, JsonArray.create().add(data.getString("email")).add(data.getString("email")).add("1111"));
        N1qlQueryResult queryResult = bucket.query(query);
        return extractResultOrThrow(queryResult);*/
    }

    public static List<Map<String, Object>> getCompanyById(final Bucket bucket, String companyId) {
        String queryStr = "SELECT _id, _type, name, address, phone, website " +
                       "FROM `" + bucket.name() + "` AS companies " +
                       "WHERE META(companies).id = $1";
        ParameterizedN1qlQuery query = ParameterizedN1qlQuery.parameterized(queryStr, JsonArray.create().add(companyId));
        N1qlQueryResult queryResult = bucket.query(query);
        return extractResultOrThrow(queryResult);
    }

    public static List<Map<String, Object>> getCompanies(final Bucket bucket) {
        String queryStr = "SELECT _id, _type, name, address, phone, website FROM `" + bucket.name() + "` WHERE _type = 'Company'";
        N1qlQueryResult queryResult = bucket.query(N1qlQuery.simple(queryStr));
        return extractResultOrThrow(queryResult);
    }

    public static ResponseEntity<String> createCompany(final Bucket bucket, JsonObject data) {
        JsonDocument document = JsonDocument.create(data.getString("website"), data.put("_id", data.getString("website")).put("_type", "Company"));
        bucket.upsert(document);
        JsonObject responseData = JsonObject.create()
                .put("success", true)
                .put("data", data);
        return new ResponseEntity<String>(responseData.toString(), HttpStatus.OK);
        /*String queryStr = "UPSERT INTO `" + bucket.name() + "` (KEY, VALUE) VALUES " +
                "($1, {'_type': 'Company', '_id': $2, 'createdON': $3, 'active': true})";
        ParameterizedN1qlQuery query = ParameterizedN1qlQuery.parameterized(queryStr, JsonArray.create().add(data.getString("website")).add(data.getString("website")).add("1111"));
        N1qlQueryResult queryResult = bucket.query(query);
        return extractResultOrThrow(queryResult);*/
    }



    public static List<Map<String, Object>> getProjectById(final Bucket bucket, String projectId) {
        String queryStr = "SELECT _id, createdON, description,name, " +
                "(SELECT _id,_type,active,address, company,createdON,name,`password`,phone " +
                "FROM `" + bucket.name() + "` USE KEYS c.owner)[0] as owner,(SELECT _id,_type,active, " +
                "address,company,createdON,name,`password`,phone FROM `" + bucket.name() + "` USE KEYS " +
                "c.users) AS users, (SELECT _id,name,description,owner,assignedTo,Users, " +
                "history,permalink FROM `" + bucket.name() + "` USE KEYS c.tasks) as tasks, permalink from " +
                " `" + bucket.name() + "` c WHERE c._id=$1";
        ParameterizedN1qlQuery query = ParameterizedN1qlQuery.parameterized(queryStr, JsonArray.create().add(projectId));
        N1qlQueryResult queryResult = bucket.query(query);
        return extractResultOrThrow(queryResult);
    }

    public static List<Map<String, Object>> getProjectsByOwnerId(final Bucket bucket, String ownerId) {
        String queryStr = "SELECT _id, _type, owner, users, tasks, description, name FROM `" + bucket.name() + "` WHERE _type = 'Project' and owner = $1";
        ParameterizedN1qlQuery query = ParameterizedN1qlQuery.parameterized(queryStr, JsonArray.create().add(ownerId));
        N1qlQueryResult queryResult = bucket.query(query);
        return extractResultOrThrow(queryResult);
    }

    public static List<Map<String, Object>> getProjects(final Bucket bucket) {
        String queryStr = "SELECT _id, _type, name, description, tasks, users, owner FROM `" + bucket.name() + "` AS projects WHERE _type = 'Project'";
        N1qlQueryResult queryResult = bucket.query(N1qlQuery.simple(queryStr));
        return extractResultOrThrow(queryResult);
    }

    public static List<Map<String, Object>> getOtherProjectsByUserId(final Bucket bucket, String userId) {
        String queryStr = "SELECT _id, _type, name, description, tasks, users, owner FROM `" + bucket.name() + "` WHERE _type = 'Project' AND ANY x IN users SATISFIES x = $1 END";
        ParameterizedN1qlQuery query = ParameterizedN1qlQuery.parameterized(queryStr, JsonArray.create().add(userId));
        N1qlQueryResult queryResult = bucket.query(query);
        return extractResultOrThrow(queryResult);
    }

    public static List<Map<String, Object>> getOtherProjects(final Bucket bucket) {
        String queryStr = "SELECT _id, _type, name, description, tasks, users, owner FROM `" + bucket.name() + "` WHERE _type = 'Project'";
        N1qlQueryResult queryResult = bucket.query(N1qlQuery.simple(queryStr));
        return extractResultOrThrow(queryResult);
    }

    public static ResponseEntity<String> createProject(final Bucket bucket, JsonObject data) {
        String documentId = UUID.randomUUID().toString();
        JsonArray users = data.getArray("users");
        users.add(data.getString("owner"));
        JsonDocument document = JsonDocument.create(documentId, data.put("_id", documentId).put("_type", "Project").put("users", users));
        bucket.upsert(document);
        JsonObject responseData = JsonObject.create()
                .put("success", true)
                .put("data", data);
        return new ResponseEntity<String>(data.toString(), HttpStatus.OK);
        /*String queryStr = "UPSERT INTO `" + bucket.name() + "` (KEY, VALUE) VALUES " +
                "($1, {'_type': 'Company', '_id': $2, 'createdON': $3, 'active': true})";
        ParameterizedN1qlQuery query = ParameterizedN1qlQuery.parameterized(queryStr, JsonArray.create().add(data.getString("website")).add(data.getString("website")).add("1111"));
        N1qlQueryResult queryResult = bucket.query(query);
        return extractResultOrThrow(queryResult);*/
    }



    public static List<Map<String, Object>> getTasksAssignedToUserId(final Bucket bucket, String userId) {
        String queryStr = "SELECT _id,(SELECT _id,_type,active," +
                "address,company,createdON,name,`password`,phone FROM `" + bucket.name() + "` USE KEYS " +
                "c.assignedTo)[0] AS assignedTo, createdON, description,history,name," +
                "(SELECT _id,_type,active,address,company,createdON,name,`password`,phone " +
                "FROM `" + bucket.name() + "` USE KEYS c.owner)[0] as owner,(SELECT _id,_type,active,address," +
                "company,createdON,name,`password`,phone FROM `" + bucket.name() + "` USE KEYS c.users) AS " +
                "users, permalink from `" + bucket.name() + "` c WHERE c.assignedTo=$1";
        ParameterizedN1qlQuery query = ParameterizedN1qlQuery.parameterized(queryStr, JsonArray.create().add(userId));
        N1qlQueryResult queryResult = bucket.query(query);
        return extractResultOrThrow(queryResult);
    }

    public static List<Map<String, Object>> getTasksAssignedTo(final Bucket bucket) {
        String queryStr = "SELECT _id,(SELECT _id,_type,active," +
                "address,company,createdON,name,`password`,phone FROM `" + bucket.name() + "` USE KEYS " +
                "c.assignedTo)[0] AS assignedTo, createdON, description,history,name," +
                "(SELECT _id,_type,active,address,company,createdON,name,`password`,phone " +
                "FROM `" + bucket.name() + "` USE KEYS c.owner)[0] as owner,(SELECT _id,_type,active,address," +
                "company,createdON,name,`password`,phone FROM `" + bucket.name() + "` USE KEYS c.users) AS " +
                "users, permalink from `" + bucket.name() + "` c";
        N1qlQueryResult queryResult = bucket.query(N1qlQuery.simple(queryStr));
        return extractResultOrThrow(queryResult);
    }



    public static List<Map<String, Object>> getTaskById(final Bucket bucket, String taskId) {
        String queryStr = "SELECT (p._id) AS projectId,(SELECT _id, " +
                "(SELECT _id,_type,active,address,company,createdON,name,`password`,phone " +
                "FROM `" + bucket.name() + "` USE KEYS c.assignedTo)[0] AS assignedTo, createdON, " +
                "description,(select t.log, t.createdAt, (SELECT _id,_type,active,address, " +
                "company,createdON,name,`password`,phone FROM `" + bucket.name() + "` USE KEYS t.`user`)[0] " +
                "as `user` from `" + bucket.name() + "` r UNNEST r.history t where r._id=$1) as history,name, " +
                "(SELECT _id,_type,active,address,company, createdON,name,`password`,phone " +
                "FROM `" + bucket.name() + "` USE KEYS c.owner)[0] as owner,(SELECT _id,_type,active,address, " +
                "company,createdON,name,`password`,phone FROM `" + bucket.name() + "` USE KEYS c.users) " +
                "AS users, permalink from `" + bucket.name() + "` c WHERE c._id=$1)[0] as task FROM `" + bucket.name() + "` " +
                "p WHERE ANY x IN tasks SATISFIES x=$1 END ";
        ParameterizedN1qlQuery query = ParameterizedN1qlQuery.parameterized(queryStr, JsonArray.create().add(taskId));
        N1qlQueryResult queryResult = bucket.query(query);
        return extractResultOrThrow(queryResult);
    }

    public static List<Map<String, Object>> createTask(final Bucket bucket, String projectId, JsonObject data) {
        String taskId = UUID.randomUUID().toString();
        JsonArray users = data.getArray("users");
        users.add(data.getString("owner"));
        JsonDocument document = JsonDocument.create(taskId, data.put("_id", taskId).put("_type", "Task").put("users", users));
        bucket.upsert(document);

        JsonDocument project = bucket.get(projectId);
        JsonObject jsonProject = project.content();
        JsonArray jsonProjectTasks = jsonProject.getArray("tasks");
        jsonProjectTasks.add(taskId);
        jsonProject.put("tasks", jsonProjectTasks);
        project = JsonDocument.create(jsonProject.getString("_id"), jsonProject);
        bucket.upsert(project);

        /*JsonObject responseData = JsonObject.create()
                .put("success", true)
                .put("data", data);
        return new ResponseEntity<String>(responseData.toString(), HttpStatus.OK);*/
        String queryStr = "SELECT c._id,c.createdON,c.name,c.description," +
                "(SELECT _id,_type,active,address,company,createdON,name,`password`,phone " +
                "FROM `" + bucket.name() + "` USE KEYS c.owner)[0] AS owner,c.status, (SELECT _id,_type," +
                "active,address,company,createdON,name,`password`,phone FROM `" + bucket.name() + "` " +
                " USE KEYS c.users) AS users, c.permalink from `" + bucket.name() + "` " +
                " c WHERE c._id=$1";
        N1qlParams params = N1qlParams.build().consistency(ScanConsistency.REQUEST_PLUS);
        ParameterizedN1qlQuery query = ParameterizedN1qlQuery.parameterized(queryStr, JsonArray.create().add(taskId), params);
        N1qlQueryResult queryResult = bucket.query(query);
        return extractResultOrThrow(queryResult);
    }

    public static List<Map<String, Object>> taskAddHistory(final Bucket bucket, JsonObject data) {
        JsonDocument task = bucket.get(data.getString("taskId"));
        JsonObject jsonTask = task.content();
        JsonArray taskHistory = jsonTask.getArray("history");
        JsonObject currentTaskHistory = JsonObject.create().put("log", data.getString("log")).put("user", data.getString("userId")).put("createdAt", (new Date()).toString());
        taskHistory.add(currentTaskHistory);
        jsonTask.put("history", taskHistory);
        task = JsonDocument.create(data.getString("taskId"), jsonTask);
        bucket.upsert(task);

        /*JsonObject responseData = JsonObject.create()
                .put("success", true)
                .put("data", data);
        return new ResponseEntity<String>(responseData.toString(), HttpStatus.OK);*/
        String queryStr = "SELECT ($1) AS log, (SELECT _id,_type,active," +
                "address,company,createdON,name,`password`,phone " +
                "FROM `" + bucket.name() + "` USE KEYS c._id)[0] AS `user`,($2) AS createdAt " +
                " FROM `" + bucket.name() + "` c WHERE c._id=$3 ";
        N1qlParams params = N1qlParams.build().consistency(ScanConsistency.REQUEST_PLUS);
        ParameterizedN1qlQuery query = ParameterizedN1qlQuery.parameterized(queryStr, JsonArray.create().add(data.getString("log")).add(currentTaskHistory.getString("createdAt")).add(data.getString("userId")), params);
        N1qlQueryResult queryResult = bucket.query(query);
        return extractResultOrThrow(queryResult);
    }




    /*
     * Get all documents in the bucket
     */
    public static List<Map<String, Object>> getAll(final Bucket bucket) {
        String queryStr = "SELECT META(users).id, firstname, lastname, email " +
                       "FROM `" + bucket.name() + "` AS users";
        N1qlQueryResult queryResult = bucket.query(N1qlQuery.simple(queryStr, N1qlParams.build().consistency(ScanConsistency.REQUEST_PLUS)));
        return extractResultOrThrow(queryResult);
    }

    /*
     * Get a particular document by its id
     */
    public static List<Map<String, Object>> getByDocumentId(final Bucket bucket, String documentId) {
        String queryStr = "SELECT firstname, lastname, email " +
                       "FROM `" + bucket.name() + "` AS users " +
                       "WHERE META(users).id = $1";
        ParameterizedN1qlQuery query = ParameterizedN1qlQuery.parameterized(queryStr, JsonArray.create().add(documentId));
        N1qlQueryResult queryResult = bucket.query(query);
        return extractResultOrThrow(queryResult);
    }

    /*
     * Delete records based on document id
     */
    public static List<Map<String, Object>> delete(final Bucket bucket, String documentId) {
        String queryStr = "DELETE " +
                "FROM `" + bucket.name() + "` AS users " +
                "WHERE META(users).id = $1";
        ParameterizedN1qlQuery query = ParameterizedN1qlQuery.parameterized(queryStr, JsonArray.create().add(documentId));
        N1qlQueryResult queryResult = bucket.query(query);
        return extractResultOrThrow(queryResult);
    }

    /*
     * Create or replace documents using an UPSERT
     */
    public static List<Map<String, Object>> save(final Bucket bucket, JsonObject data) {
        String documentId = !data.getString("document_id").equals("") ? data.getString("document_id") : UUID.randomUUID().toString();
        String queryStr = "UPSERT INTO `" + bucket.name() + "` (KEY, VALUE) VALUES " +
                "($1, {'firstname': $2, 'lastname': $3, 'email': $4})";
        JsonArray parameters = JsonArray.create()
                .add(documentId)
                .add(data.getString("firstname"))
                .add(data.getString("lastname"))
                .add(data.getString("email"));
        ParameterizedN1qlQuery query = ParameterizedN1qlQuery.parameterized(queryStr, parameters);
        N1qlQueryResult queryResult = bucket.query(query);
        return extractResultOrThrow(queryResult);
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
