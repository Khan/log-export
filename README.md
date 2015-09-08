# log-export

A Google Cloud Dataflow streaming job to export Khan Academy logs to BigQuery.

To get started, first follow the [Cloud Dataflow instructions](https://cloud.google.com/dataflow/getting-started#DevEnv) to set up your dependencies.
You will need a Java 8 JDK and Maven to build the project, and you need a properly-authenticated gcloud on your PATH for deployment to work.

There are two `make` targets:

* `make deploy_test` deploys a dataflow job pointed at a temporary subscription and a test output table.
* `make deploy_prod` deploys a dataflow job pointed at the production subscription and output table.

See the documentation in the makefile for more details.