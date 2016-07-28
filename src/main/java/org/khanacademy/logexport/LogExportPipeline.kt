package org.khanacademy.logexport

import com.google.api.client.json.JsonFactory
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.bigquery.model.TableRow
import com.google.api.services.logging.v2beta1.model.LogEntry
import com.google.cloud.dataflow.sdk.Pipeline
import com.google.cloud.dataflow.sdk.io.BigQueryIO
import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.CreateDisposition
import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.WriteDisposition
import com.google.cloud.dataflow.sdk.io.PubsubIO
import com.google.cloud.dataflow.sdk.options.*
import com.google.cloud.dataflow.sdk.transforms.DoFn
import com.google.cloud.dataflow.sdk.transforms.ParDo

/**
 * Entry point for the Log Export process.
 */
object LogExportPipeline {
    private val JSON_FACTORY = JacksonFactory()

    @Description("Options that configure the Log Export pipeline.")
    interface LogExportOptions : PipelineOptions {
        var subscription: String?

        var topic: String?

        var runBoundedOver: Int?

        var outputTable: String
    }

    @JvmStatic fun main(args: Array<String>) {
        PipelineOptionsFactory.register(LogExportOptions::class.java)

        val pipelineOptions = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .create()
        val logExportOptions = pipelineOptions
                .`as`<LogExportOptions>(LogExportOptions::class.java)
        PipelineOptionsValidator.validate<LogExportOptions>(LogExportOptions::class.java, logExportOptions)

        val dataflowOptions = pipelineOptions
                .`as`<DataflowPipelineOptions>(DataflowPipelineOptions::class.java)
        dataflowOptions.isStreaming = true

        val pipeline = Pipeline.create(pipelineOptions)

        val subscription = logExportOptions.subscription
        val topic = logExportOptions.topic
        val runBound = logExportOptions.runBoundedOver

        if (subscription == null == (topic == null)) {
            throw IllegalArgumentException(
                    "Exactly one of subscription and topic should be specified.")
        }

        var pubSubConfig: PubsubIO.Read.Bound<String> = PubsubIO.Read.named("ReadFromPubSub")
        if (subscription != null) {
            pubSubConfig = pubSubConfig.subscription(subscription)
        } else {
            pubSubConfig = pubSubConfig.topic(topic!!)
        }

        if (runBound != null) {
            pubSubConfig = pubSubConfig.maxNumRecords(runBound)
        }

        pipeline.apply(pubSubConfig)
                .apply(ParDo.of(ExtractLogs()))
                .apply(BigQueryIO.Write.named("WriteToBigQuery")
                        .to(logExportOptions.outputTable)
                        .withWriteDisposition(WriteDisposition.WRITE_APPEND)
                        .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                        .withSchema(LogsExtractor.create().schema))

        pipeline.run()
    }

    private class ExtractLogs : DoFn<String, TableRow>() {
        // Logs extraction logic, initialized separately for each shard.
        @Transient private var logsExtractor: LogsExtractor? = null

        @Throws(Exception::class)
        override fun startBundle(c: DoFn<String, TableRow>.Context?) {
            logsExtractor = LogsExtractor.create()
        }

        @Throws(Exception::class)
        override fun processElement(c: DoFn<String, TableRow>.ProcessContext) {
            val logJson = c.element()
            val parsedLog = JSON_FACTORY.fromString<LogEntry>(logJson, LogEntry::class.java)
            c.output(logsExtractor!!.extractLogs(parsedLog))
        }
    }
}
