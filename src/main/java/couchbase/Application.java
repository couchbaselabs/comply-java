package couchbase;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.json.JsonObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import javax.servlet.*;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;


@SpringBootApplication
@RestController
@RequestMapping("/api")
public class Application implements Filter {

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @Override
    public void doFilter(ServletRequest req, ServletResponse res, FilterChain chain)
            throws IOException, ServletException {
        HttpServletResponse response = (HttpServletResponse) res;
        response.setHeader("Access-Control-Allow-Origin", "*");
        response.setHeader("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
        chain.doFilter(req, res);
    }

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {}

    @Override
    public void destroy() {}

    @Value("${hostname}")
    private String hostname;

    @Value("${bucket}")
    private String bucket;

    @Value("${password}")
    private String password;

    public @Bean
    Cluster cluster() {
        return CouchbaseCluster.create(hostname);
    }

    public @Bean
    Bucket bucket() {
        return cluster().openBucket(bucket, password);
    }



    @RequestMapping(value="/user/get/{userId}", method= RequestMethod.GET)
    public Object getUserById(@PathVariable("userId") String userId) {
        return Database.getUserById(bucket(), userId);
    }

    @RequestMapping(value="/user/getAll", method= RequestMethod.GET)
    public Object getUsers() {
        return Database.getUsers(bucket());
    }

    @RequestMapping(value="/user/login/{email}/{password}", method= RequestMethod.GET)
    public Object login(@PathVariable("email") String email, @PathVariable("password") String password) {
        return Database.login(bucket(), email, password);
    }

    @RequestMapping(value="/user/create", method= RequestMethod.POST)
    public Object createUser(@RequestBody String json) {
        JsonObject jsonData = JsonObject.fromJson(json);
        return Database.createUser(bucket(), jsonData);
    }



    @RequestMapping(value="/company/get/{companyId}", method= RequestMethod.GET)
    public Object getCompanyById(@PathVariable("companyId") String companyId) {
        return Database.getCompanyById(bucket(), companyId);
    }

    @RequestMapping(value="/company/getAll", method= RequestMethod.GET)
    public Object getCompanies() {
        return Database.getCompanies(bucket());
    }

    @RequestMapping(value="/company/create", method= RequestMethod.POST)
    public Object createCompany(@RequestBody String json) {
        JsonObject jsonData = JsonObject.fromJson(json);
        return Database.createCompany(bucket(), jsonData);
    }


    @RequestMapping(value="/project/get/{projectId}", method= RequestMethod.GET)
    public Object getProjectById(@PathVariable("projectId") String projectId) {
        return Database.getProjectById(bucket(), projectId);
    }

    @RequestMapping(value="/project/getAll/{ownerId}", method= RequestMethod.GET)
    public Object getProjectsByOwnerId(@PathVariable("ownerId") String ownerId) {
        return Database.getProjectsByOwnerId(bucket(), ownerId);
    }

    @RequestMapping(value="/project/getAll", method= RequestMethod.GET)
    public Object getProjects() {
        return Database.getProjects(bucket());
    }

    @RequestMapping(value="/project/getOther/{userId}", method= RequestMethod.GET)
    public Object getOtherProjectsByUserId(@PathVariable("userId") String userId) {
        System.out.println(userId);
        return Database.getOtherProjectsByUserId(bucket(), userId);
    }

    @RequestMapping(value="/project/getOther", method= RequestMethod.GET)
    public Object getOtherProjects() {
        return Database.getOtherProjects(bucket());
    }

    @RequestMapping(value="/project/create", method= RequestMethod.POST)
    public Object createProject(@RequestBody String json) {
        JsonObject jsonData = JsonObject.fromJson(json);
        return Database.createProject(bucket(), jsonData);
    }

    @RequestMapping(value="/project/addUser", method= RequestMethod.POST)
    public Object projectAddUser(@RequestBody String json) {
        JsonObject jsonData = JsonObject.fromJson(json);
        return Database.projectAddUser(bucket(), jsonData);
    }





    @RequestMapping(value="/task/get/{taskId}", method= RequestMethod.GET)
    public Object getTaskById(@PathVariable("taskId") String taskId) {
        return Database.getTaskById(bucket(), taskId);
    }

    @RequestMapping(value="/task/getAssignedTo/{userId}", method= RequestMethod.GET)
    public Object getTasksAssignedToUserId(@PathVariable("userId") String userId) {
        return Database.getTasksAssignedToUserId(bucket(), userId);
    }

    @RequestMapping(value="/task/getAssignedTo", method= RequestMethod.GET)
    public Object getTasksAssignedTo(@PathVariable("userId") String userId) {
        return Database.getTasksAssignedTo(bucket());
    }

    @RequestMapping(value="/task/create/{projectId}", method= RequestMethod.POST)
    public Object createTaskForProjectId(@PathVariable("projectId") String projectId, @RequestBody String json) {
        JsonObject jsonData = JsonObject.fromJson(json);
        return Database.createTask(bucket(), projectId, jsonData);
    }

    @RequestMapping(value="/task/addUser", method= RequestMethod.POST)
    public Object taskAddUser(@RequestBody String json) {
        JsonObject jsonData = JsonObject.fromJson(json);
        return Database.taskAddUser(bucket(), jsonData);
    }

    @RequestMapping(value="/task/assignUser", method= RequestMethod.POST)
    public Object taskAssignUser(@RequestBody String json) {
        JsonObject jsonData = JsonObject.fromJson(json);
        return Database.taskAssignUser(bucket(), jsonData);
    }

    @RequestMapping(value="/task/addHistory", method= RequestMethod.POST)
    public Object taskAddHistory(@RequestBody String json) {
        JsonObject jsonData = JsonObject.fromJson(json);
        return Database.taskAddHistory(bucket(), jsonData);
    }

}
