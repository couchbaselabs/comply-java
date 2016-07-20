# Couchbase Compliance Demo with Java

This project is meant to demonstrate a full stack application using Java, Angular 2, and Couchbase Server.  This project shows separation of the frontend and backend code as well as highly relational, yet schema-less NoSQL document data.

## The Requirements

There are a few requirements that must be met before trying to run this project:

* The Angular 2 CLI
* Node.js 4.0+
* JDK 1.8+
* Couchbase Server 4.1+
* Maven

The Angular CLI is necessary to build the Angular 2 frontend that is provided with this project.  The JDK and Maven are required for building the Java backend.  Couchbase Server is required as the database layer.

## Configuring the Project

There are three things that must be done to run this project:

1. The Angular 2 frontend must be built
2. The Java project must be compiled
3. The database must be configured

The Angular 2 code is not dependent on Java or the database.  To build the Angular 2 project, execute the following from the **angular** directory of the project:

```sh
npm install
ng build
```

The above commands will install the Angular 2 dependencies and build the **angular/src** to the **angular/dist** directory.  Copy this directory to the parent directory like the following:

```sh
cp -r angular/dist public
```

Notice that the directory must be renamed to **public** at the root of the project.

Before compiling the Java code, the Couchbase Server information must be specified first.  In the project's **src/main/resources/application.properties** file, take a look at the details:

```
hostname=127.0.0.1
bucket=default
password=
```

Adjust the host and bucket information to match that of your own.

To build the Java project, execute the following from your Command Prompt (Windows) or Terminal (Linux and Mac):

```sh
mvn package
```

Because this project makes use of N1QL, the bucket that you define must have at least one index.

## Running the Project

Provided that the Angular 2 code has been built and you've configured your database information in the project's **src/main/resources/application.properties** file, execute the following:

```sh
mvn spring-boot:run
```

From the web browser, navigate to **http://localhost:8080** to see the application in action.

## Resources

Couchbase - [http://www.couchbase.com](http://www.couchbase.com)

Couchbase Compliance Demo with Node.js - [https://github.com/couchbaselabs/comply](https://github.com/couchbaselabs/comply)

Couchbase Compliance Demo with GoLang - [https://github.com/couchbaselabs/comply-golang](https://github.com/couchbaselabs/comply-golang)
