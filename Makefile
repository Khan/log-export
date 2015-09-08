# Deploy a dataflow pipeline hooked up against the real production data, except
# that it writes to a test output table and doesn't use the production
# subscription (and thus doesn't interfere with the production job).
deploy_test:
	mvn compile exec:java \
		-Dexec.mainClass=org.khanacademy.logexport.LogExportPipeline \
		-Dexec.args=" \
            --project=khan-academy \
            --stagingLocation=gs://khan_academy_dataflow \
            --runner=BlockingDataflowPipelineRunner \
            --topic=projects/khan-academy/topics/request_logs \
            --outputTable=khan-academy:logs_streaming_test.test_logs"

# Deploy a dataflow pipeline using the production subscription and output table.
# Does NOT automatically stop any existing production pipelines that are
# running; you need to go into the dataflow UI and stop them directly. If a
# pipeline is running, the log processing work will be split between the two
# pipelines, since they will share a subscription. If no pipeline is running, a
# backlog of work will accumulate on the subscription and this pipeline will
# begin processing that backlog.
deploy_prod:
	mvn compile exec:java \
		-Dexec.mainClass=org.khanacademy.logexport.LogExportPipeline \
		-Dexec.args=" \
            --project=khan-academy \
            --stagingLocation=gs://khan_academy_dataflow \
            --runner=BlockingDataflowPipelineRunner \
            --subscription=projects/khan-academy/subscriptions/log_export_prod \
            --outputTable=khan-academy:logs_streaming.logs_all_time"

