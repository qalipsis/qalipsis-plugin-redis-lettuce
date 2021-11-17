package io.qalipsis.plugins.redis.lettuce.streams.consumer

import io.lettuce.core.StreamMessage
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.qalipsis.api.context.StepOutput
import io.qalipsis.api.context.StepStartStopContext
import io.qalipsis.api.lang.tryAndLogOrNull
import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.api.steps.datasource.DatasourceObjectConverter
import java.util.concurrent.atomic.AtomicLong

/**
 * Implementation of [DatasourceObjectConverter], that reads a message of native Redis Streams records and forwards
 * it converted as a list of [LettuceStreamsConsumedRecord].
 *
 * @author Gabriel Moraes
 */
internal class LettuceStreamsConsumerBatchConverter(
    private val meterRegistry: MeterRegistry?
) : DatasourceObjectConverter<List<StreamMessage<ByteArray, ByteArray>>, LettuceStreamsConsumerResult> {

    private val meterPrefix = "redis-lettuce-streams-consumer"

    private var recordsCounter: Counter? = null

    private var valuesBytesReceived: Counter? = null

    override fun start(context: StepStartStopContext) {
        meterRegistry?.apply {
            val tags = context.toMetersTags()
            recordsCounter = counter("$meterPrefix-records", tags)
            valuesBytesReceived = counter("$meterPrefix-records-bytes", tags)
        }
    }

    override fun stop(context: StepStartStopContext) {
        meterRegistry?.apply {
            remove(recordsCounter!!)
            remove(valuesBytesReceived!!)
            recordsCounter = null
            valuesBytesReceived = null
        }
    }

    override suspend fun supply(
        offset: AtomicLong, value: List<StreamMessage<ByteArray, ByteArray>>,
        output: StepOutput<LettuceStreamsConsumerResult>
    ) {

        tryAndLogOrNull(log) {
            output.send(
                LettuceStreamsConsumerResult(
                    records = value.map { record ->

                        valuesBytesReceived?.increment(record.body.values.sumOf { it.size }.toDouble())
                        recordsCounter?.increment()

                        LettuceStreamsConsumedRecord(
                            offset.getAndIncrement(),
                            record.id,
                            record.stream,
                            record.body.map { it.key.decodeToString() to it.value.decodeToString() }.toMap()
                        )
                    },
                    meters = LettuceStreamsConsumerMetrics(
                        recordsCounter,
                        valuesBytesReceived
                    )
                )
            )
        }
    }

    companion object {

        @JvmStatic
        private val log = logger()
    }
}
