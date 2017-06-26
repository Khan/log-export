PROJECT_NAME = khan-academy
STAGING_LOCATION = gs://khan_academy_dataflow
MACHINE_TYPE = n1-standard-4

run_locally:
	MAVEN_ARGS="-Xmx1G" mvn compile exec\:java \
		-Dexec.mainClass=org.khanacademy.logexport.LogExportPipeline \
		-Dexec.args=" \
            --project=$(PROJECT_NAME) \
            --runner=DirectPipelineRunner \
            --runBoundedOver=1000 \
            --topic=projects/khan-academy/topics/request_logs \
            --outputTable=khan-academy:logs_streaming_test.test_logs"

# Deploy a dataflow pipeline hooked up against the real production data, except
# that it writes to a test output table and doesn't use the production
# subscription (and thus doesn't interfere with the production job).
# This version does not exit and writes job logs to the console.
deploy_test:
	./update_schema.py -t khan-academy.logs_streaming_test.test_logs
	mvn compile exec\:java \
		-Dexec.mainClass=org.khanacademy.logexport.LogExportPipeline \
		-Dexec.args=" \
            --project=$(PROJECT_NAME) \
            --stagingLocation=$(STAGING_LOCATION) \
            --workerMachineType=$(MACHINE_TYPE) \
            --runner=BlockingDataflowPipelineRunner \
            --topic=projects/khan-academy/topics/request_logs \
            --outputTable=khan-academy:logs_streaming_test.test_logs"

# Deploy a dataflow pipeline hooked up against the real production data, except
# that it writes to a test output table and doesn't use the production
# subscription (and thus doesn't interfere with the production job).
# This version exits as soon as the job has started and doesn't output job logs.
deploy_test_nonblocking:
	./update_schema.py -t khan-academy.logs_streaming_test.test_logs
	mvn compile exec\:java \
		-Dexec.mainClass=org.khanacademy.logexport.LogExportPipeline \
		-Dexec.args=" \
            --project=$(PROJECT_NAME) \
            --stagingLocation=$(STAGING_LOCATION) \
            --workerMachineType=$(MACHINE_TYPE) \
            --runner=DataflowPipelineRunner \
            --topic=projects/khan-academy/topics/request_logs \
            --outputTable=khan-academy:logs_streaming_test.test_logs"

# Deploy a dataflow pipeline using the production subscription and output table.
# Does NOT automatically stop any existing production pipelines that are
# running; you need to go into the dataflow UI and stop them directly:
# https://console.cloud.google.com/dataflow?project=khan-academy. To stop a
# job, click the job; then, under 'Summary', on the 'Job Status' line, click
# 'Stop job' next to 'Running'. When given the choice, be sure to select the
# option to drain the remaining buffered data.
# If a pipeline is running, the log processing work will be split between the
# two pipelines, since they will share a subscription. If no pipeline is
# running, a backlog of work will accumulate on the subscription and this
# pipeline will begin processing that backlog.
deploy_prod:
	./update_schema.py -t khan-academy.logs_streaming.logs_all_time
	mvn compile exec\:java \
		-Dexec.mainClass=org.khanacademy.logexport.LogExportPipeline \
		-Dexec.args=" \
            --numWorkers=5 \
            --project=$(PROJECT_NAME) \
            --stagingLocation=$(STAGING_LOCATION) \
            --workerMachineType=$(MACHINE_TYPE) \
            --runner=DataflowPipelineRunner \
            --subscription=projects/khan-academy/subscriptions/log_export \
            --outputTable=khan-academy:logs_streaming.logs_all_time"
	@echo "Now visit"
	@echo "   https://console.cloud.google.com/dataflow?project=khan-academy"
	@echo "to disable the old dataflow job"
