package couchbase.models;

import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;

public class User {

    private JsonObject json;

    public User(JsonObject json) {
        this.json = JsonObject.create();
        this.json
            .put("name", 1);
    }

    public void setName(JsonObject name) {

    }

}
