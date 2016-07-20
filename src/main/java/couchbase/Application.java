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

    /*
     * ###################################################
     * ################# User Endpoints ##################
     * ###################################################
     */


    /*
     * Endpoint for getting a particular user from the database by its user id
     */
    @RequestMapping(value="/user/get/{userId}", method= RequestMethod.GET)
    public Object getUserById(@PathVariable("userId") String userId) {
        return Database.getUserById(bucket(), userId);
    }

    /*
     * Endpoint for getting all users from the database
     */
    @RequestMapping(value="/user/getAll", method= RequestMethod.GET)
    public Object getUsers() {
        return Database.getUsers(bucket());
    }

    /*
     * Endpoint for signing the user into the application
     */
    @RequestMapping(value="/user/login/{username}/{password}", method= RequestMethod.GET)
    public Object login(@PathVariable("username") String username, @PathVariable("password") String password) {
        if(username.equals("")) {
            return new ResponseEntity<String>(JsonObject.create().put("error", 400).put("message", "A username must exist").toString(), HttpStatus.BAD_REQUEST);
        } else if(password.equals("")) {
            return new ResponseEntity<String>(JsonObject.create().put("error", 400).put("message", "A password must exist").toString(), HttpStatus.BAD_REQUEST);
        }
        return Database.login(bucket(), username, password);
    }

    /*
     * Endpoint for creating a new user
     */
    @RequestMapping(value="/user/create", method= RequestMethod.POST)
    public Object createUser(@RequestBody String json) {
        JsonObject jsonData = JsonObject.fromJson(json);
        if(!jsonData.containsKey("username")) {
            return new ResponseEntity<String>(JsonObject.create().put("error", 400).put("message", "A username must exist").toString(), HttpStatus.BAD_REQUEST);
        } else if(!jsonData.containsKey("password")) {
            return new ResponseEntity<String>(JsonObject.create().put("error", 400).put("message", "A password must exist").toString(), HttpStatus.BAD_REQUEST);
        }
        return Database.createUser(bucket(), jsonData, jsonData.getString("username"), jsonData.getString("password"));
    }


    /*
     * ###################################################
     * ################ Company Endpoints ################
     * ###################################################
     */


    /*
     * Endpoint for getting a particular company from the database
     */
    @RequestMapping(value="/company/get/{companyId}", method= RequestMethod.GET)
    public Object getCompanyById(@PathVariable("companyId") String companyId) {
        if(companyId.equals("")) {
            return new ResponseEntity<String>(JsonObject.create().put("error", 400).put("message", "A company id must exist").toString(), HttpStatus.BAD_REQUEST);
        }
        return Database.getCompanyById(bucket(), companyId);
    }

    /*
     * Endpoint for retrieving all companies from the database
     */
    @RequestMapping(value="/company/getAll", method= RequestMethod.GET)
    public Object getCompanies() {
        return Database.getCompanies(bucket());
    }

    /*
     * Endpoint for creating a company
     */
    @RequestMapping(value="/company/create", method= RequestMethod.POST)
    public Object createCompany(@RequestBody String json) {
        JsonObject jsonData = JsonObject.fromJson(json);
        if(!jsonData.containsKey("website")) {
            return new ResponseEntity<String>(JsonObject.create().put("error", 400).put("message", "A website must exist").toString(), HttpStatus.BAD_REQUEST);
        } else if(!jsonData.containsKey("name")) {
            return new ResponseEntity<String>(JsonObject.create().put("error", 400).put("message", "A name must exist").toString(), HttpStatus.BAD_REQUEST);
        }
        return Database.createCompany(bucket(), jsonData);
    }


    /*
     * ###################################################
     * ################ Project Endpoints ################
     * ###################################################
     */


    /*
     * Endpoint to get a particular project by its id
     */
    @RequestMapping(value="/project/get/{projectId}", method= RequestMethod.GET)
    public Object getProjectById(@PathVariable("projectId") String projectId) {
        if(projectId.equals("")) {
            return new ResponseEntity<String>(JsonObject.create().put("error", 400).put("message", "A project id must exist").toString(), HttpStatus.BAD_REQUEST);
        }
        return Database.getProjectById(bucket(), projectId);
    }

    /*
     * Endpoint to get all projects owned by a particular user id
     */
    @RequestMapping(value="/project/getAll/{ownerId}", method= RequestMethod.GET)
    public Object getProjectsByOwnerId(@PathVariable("ownerId") String ownerId) {
        if(ownerId.equals("")) {
            return new ResponseEntity<String>(JsonObject.create().put("error", 400).put("message", "An owner id must exist").toString(), HttpStatus.BAD_REQUEST);
        }
        return Database.getProjectsByOwnerId(bucket(), ownerId);
    }

    /*
     * Endpoint for retrieving all projects from the database
     */
    @RequestMapping(value="/project/getAll", method= RequestMethod.GET)
    public Object getProjects() {
        return Database.getProjects(bucket());
    }

    /*
     * Endpoint for getting projects not owned by a particular user, but still in some relationship to the user id
     */
    @RequestMapping(value="/project/getOther/{userId}", method= RequestMethod.GET)
    public Object getOtherProjectsByUserId(@PathVariable("userId") String userId) {
        if(userId.equals("")) {
            return new ResponseEntity<String>(JsonObject.create().put("error", 400).put("message", "A user id must exist").toString(), HttpStatus.BAD_REQUEST);
        }
        return Database.getOtherProjectsByUserId(bucket(), userId);
    }

    /*
     * Endpoint to get all other projects, which is really just getting all projects in case the request was bad
     */
    @RequestMapping(value="/project/getOther", method= RequestMethod.GET)
    public Object getOtherProjects() {
        return Database.getProjects(bucket());
    }

    /*
     * Endpoint for creating a project
     */
    @RequestMapping(value="/project/create", method= RequestMethod.POST)
    public Object createProject(@RequestBody String json) {
        JsonObject jsonData = JsonObject.fromJson(json);
        if(!jsonData.containsKey("owner")) {
            return new ResponseEntity<String>(JsonObject.create().put("error", 400).put("message", "An owner must exist").toString(), HttpStatus.BAD_REQUEST);
        } else if(!jsonData.containsKey("users")) {
            return new ResponseEntity<String>(JsonObject.create().put("error", 400).put("message", "Users must exist").toString(), HttpStatus.BAD_REQUEST);
        } else if(!jsonData.containsKey("name")) {
            return new ResponseEntity<String>(JsonObject.create().put("error", 400).put("message", "A name must exist").toString(), HttpStatus.BAD_REQUEST);
        } else if(!jsonData.containsKey("description")) {
            return new ResponseEntity<String>(JsonObject.create().put("error", 400).put("message", "A description must exist").toString(), HttpStatus.BAD_REQUEST);
        }
        return Database.createProject(bucket(), jsonData);
    }

    /*
     * Endpoint for adding an existing user to a project
     */
    @RequestMapping(value="/project/addUser", method= RequestMethod.POST)
    public Object projectAddUser(@RequestBody String json) {
        JsonObject jsonData = JsonObject.fromJson(json);
        if(!jsonData.containsKey("username")) {
            return new ResponseEntity<String>(JsonObject.create().put("error", 400).put("message", "An username must exist").toString(), HttpStatus.BAD_REQUEST);
        } else if(!jsonData.containsKey("projectId")) {
            return new ResponseEntity<String>(JsonObject.create().put("error", 400).put("message", "A project id must exist").toString(), HttpStatus.BAD_REQUEST);
        }
        return Database.projectAddUser(bucket(), jsonData);
    }


    /*
     * ###################################################
     * ################# Task Endpoints ##################
     * ###################################################
     */


    /*
     * Endpoint for retrieving a task based on a task id
     */
    @RequestMapping(value="/task/get/{taskId}", method= RequestMethod.GET)
    public Object getTaskById(@PathVariable("taskId") String taskId) {
        if(taskId.equals("")) {
            return new ResponseEntity<String>(JsonObject.create().put("error", 400).put("message", "A task id must exist").toString(), HttpStatus.BAD_REQUEST);
        }
        return Database.getTaskById(bucket(), taskId);
    }

    /*
     * Endpoint for retrieving tasks assigned to a particular user
     */
    @RequestMapping(value="/task/getAssignedTo/{userId}", method= RequestMethod.GET)
    public Object getTasksAssignedToUserId(@PathVariable("userId") String userId) {
        if(userId.equals("")) {
            return new ResponseEntity<String>(JsonObject.create().put("error", 400).put("message", "A user id must exist").toString(), HttpStatus.BAD_REQUEST);
        }
        return Database.getTasksAssignedToUserId(bucket(), userId);
    }

    /*
     * Endpoint for creating a new task for an already existing project
     */
    @RequestMapping(value="/task/create/{projectId}", method= RequestMethod.POST)
    public Object createTaskForProjectId(@PathVariable("projectId") String projectId, @RequestBody String json) {
        JsonObject jsonData = JsonObject.fromJson(json);
        if(!jsonData.containsKey("users")) {
            return new ResponseEntity<String>(JsonObject.create().put("error", 400).put("message", "Users must exist").toString(), HttpStatus.BAD_REQUEST);
        } else if(!jsonData.containsKey("owner")) {
            return new ResponseEntity<String>(JsonObject.create().put("error", 400).put("message", "An owner must exist").toString(), HttpStatus.BAD_REQUEST);
        } else if(projectId.equals("")) {
            return new ResponseEntity<String>(JsonObject.create().put("error", 400).put("message", "A project id must exist").toString(), HttpStatus.BAD_REQUEST);
        }
        return Database.createTask(bucket(), projectId, jsonData);
    }

    /*
     * Endpoint for adding an existing user to an existing task
     */
    @RequestMapping(value="/task/addUser", method= RequestMethod.POST)
    public Object taskAddUser(@RequestBody String json) {
        JsonObject jsonData = JsonObject.fromJson(json);
        if(!jsonData.containsKey("username")) {
            return new ResponseEntity<String>(JsonObject.create().put("error", 400).put("message", "A username must exist").toString(), HttpStatus.BAD_REQUEST);
        } else if(!jsonData.containsKey("taskId")) {
            return new ResponseEntity<String>(JsonObject.create().put("error", 400).put("message", "A task id must exist").toString(), HttpStatus.BAD_REQUEST);
        }
        return Database.taskAddUser(bucket(), jsonData);
    }

    /*
     * Endpoint for assigning an existing user to an existing task
     */
    @RequestMapping(value="/task/assignUser", method= RequestMethod.POST)
    public Object taskAssignUser(@RequestBody String json) {
        JsonObject jsonData = JsonObject.fromJson(json);
        if(!jsonData.containsKey("userId")) {
            return new ResponseEntity<String>(JsonObject.create().put("error", 400).put("message", "A user id must exist").toString(), HttpStatus.BAD_REQUEST);
        } else if(!jsonData.containsKey("taskId")) {
            return new ResponseEntity<String>(JsonObject.create().put("error", 400).put("message", "A task id must exist").toString(), HttpStatus.BAD_REQUEST);
        }
        return Database.taskAssignUser(bucket(), jsonData);
    }

    /*
     * Endpoint for adding comment history to an existing task
     */
    @RequestMapping(value="/task/addHistory", method= RequestMethod.POST)
    public Object taskAddHistory(@RequestBody String json) {
        JsonObject jsonData = JsonObject.fromJson(json);
        if(!jsonData.containsKey("taskId")) {
            return new ResponseEntity<String>(JsonObject.create().put("error", 400).put("message", "A task id must exist").toString(), HttpStatus.BAD_REQUEST);
        } else if(!jsonData.containsKey("userId")) {
            return new ResponseEntity<String>(JsonObject.create().put("error", 400).put("message", "A user id must exist").toString(), HttpStatus.BAD_REQUEST);
        } else if(!jsonData.containsKey("log")) {
            return new ResponseEntity<String>(JsonObject.create().put("error", 400).put("message", "A log must exist").toString(), HttpStatus.BAD_REQUEST);
        }
        return Database.taskAddHistory(bucket(), jsonData);
    }

}
