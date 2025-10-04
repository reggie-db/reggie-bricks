package com.reggie.bricks.tools

import com.databricks.zerobus.IngestRecordResponse
import com.databricks.zerobus_sdk.IngestRecordResult
import com.databricks.zerobus_sdk.StreamConfigurationOptions
import com.databricks.zerobus_sdk.TableProperties
import com.databricks.zerobus_sdk.ZerobusException
import com.databricks.zerobus_sdk.ZerobusSdk
import com.databricks.zerobus_sdk.ZerobusStream
import jakarta.annotation.PreDestroy
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import java.util.Base64
import java.util.concurrent.CompletableFuture
import java.util.function.Consumer


@Service
class ZerobusService(
    @param:Value("\${zerobus.endpoint}") private val zerobusEndpoint: String,
    @param:Value("\${zerobus.ucEndpoint}") private val unityCatalogEndpoint: String,
    @param:Value("\${zerobus.ucToken}") private val ucToken: String,
    @param:Value("\${zerobus.maxInflight:50000}") private val maxInflight: Int
) {
    private val log = LoggerFactory.getLogger(ZerobusService::class.java)
    private val sdk by lazy { ZerobusSdk(zerobusEndpoint, unityCatalogEndpoint, ucToken) }
    private val openStreams = mutableMapOf<String, ZerobusStream<*>>()

    /**
     * Decode a Base64 string that may be a data URL. Returns raw bytes.
     */
    private fun decodeBase64Bytes(input: String): ByteArray {
        val base64Part = input.substringAfter("base64,", missingDelimiterValue = input)
        return Base64.getDecoder().decode(base64Part.trim())
    }

    /**
     * Ingest a single image into Zerobus using the provided record builder for your schema.
     *
     * @param base64OrDataUrl image as raw base64 or data URL
     * @param tableName fully qualified UC table (e.g., catalog.schema.table)
     * @param recordClass the concrete record class used by your Zerobus table
     * @param builder function that builds a record instance from the decoded bytes
     * @return the ingestion result
     */
    fun <T : Any> ingestImage(
        base64OrDataUrl: String,
        tableName: String,
        recordClass: Class<T>,
        builder: (imageBytes: ByteArray) -> T
    ): IngestRecordResult {
        val imageBytes = decodeBase64Bytes(base64OrDataUrl)

        val ackCallback = Consumer<IngestRecordResponse> { resp ->
            log.debug("Ack up to offset {}", resp.durabilityAckUpToOffset)
        }

        val options = StreamConfigurationOptions.builder()
            .setMaxInflightRecords(maxInflight)
            .setAckCallback(ackCallback)
            .build()

        val streamKey = "$tableName::${recordClass.name}"
        @Suppress("UNCHECKED_CAST")
        val stream = openStreams.getOrPut(streamKey) {
            val tp = TableProperties(tableName, IotIngestRaw.RawMessage::class.java)
            sdk.createStream(tp, options)
        } as ZerobusStream<T>

        return try {
            val record = builder(imageBytes)
            val start = System.currentTimeMillis()

            val result = stream.ingestRecord(record)

            // Ensure SDK accepted the record before returning
            result.recordAccepted.get()

            // Trigger durability asynchronously but make sure we flush to push buffers
            stream.flush()

            // Optionally wait for durability here
            waitFor(listOf(result.writeCompleted))

            val elapsed = System.currentTimeMillis() - start
            log.info("Ingested image to {} in {} ms", tableName, elapsed)

            result
        } catch (e: ZerobusException) {
            log.error("Zerobus error while ingesting image to {}", tableName, e)
            throw e
        } catch (e: Exception) {
            log.error("Unexpected error while ingesting image to {}", tableName, e)
            throw e
        }
    }

    private fun waitFor(futures: Collection<CompletableFuture<*>>) {
        for (f in futures) {
            try {
                f.get()
            } catch (e: Exception) {
                log.warn("Durability future completed with exception", e)
                throw e
            }
        }
    }

    @PreDestroy
    fun close() {
        openStreams.values.forEach {
            try {
                it.flush()
            } catch (e: Exception) {
                log.debug("Flush on close failed", e)
            } finally {
                try {
                    it.close()
                } catch (e: Exception) {
                    log.debug("Close stream failed", e)
                }
            }
        }
        openStreams.clear()
    }
}