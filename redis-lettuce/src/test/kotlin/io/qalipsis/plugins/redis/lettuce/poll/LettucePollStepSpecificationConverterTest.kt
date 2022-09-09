package io.qalipsis.plugins.redis.lettuce.poll

import assertk.all
import assertk.assertThat
import assertk.assertions.isEqualTo
import assertk.assertions.isFalse
import assertk.assertions.isInstanceOf
import assertk.assertions.isNotNull
import assertk.assertions.isSameAs
import assertk.assertions.isTrue
import io.aerisconsulting.catadioptre.getProperty
import io.mockk.impl.annotations.RelaxedMockK
import io.qalipsis.api.steps.StepCreationContext
import io.qalipsis.api.steps.StepCreationContextImpl
import io.qalipsis.api.steps.StepMonitoringConfiguration
import io.qalipsis.api.steps.datasource.IterativeDatasourceStep
import io.qalipsis.api.steps.datasource.processors.NoopDatasourceObjectProcessor
import io.qalipsis.plugins.redis.lettuce.poll.converters.PollResultSetBatchConverter
import io.qalipsis.plugins.redis.lettuce.poll.converters.RedisToJavaConverter
import io.qalipsis.plugins.redis.lettuce.poll.scanners.LettuceKeysScanner
import io.qalipsis.test.assertk.prop
import io.qalipsis.test.mockk.WithMockk
import io.qalipsis.test.mockk.relaxedMockk
import io.qalipsis.test.steps.AbstractStepSpecificationConverterTest
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import java.time.Duration

/**
 *
 * @author Gabriel Moraes
 */
@WithMockk
internal class LettucePollStepSpecificationConverterTest :
    AbstractStepSpecificationConverterTest<LettucePollStepSpecificationConverter>() {

    @RelaxedMockK
    private lateinit var lettucePollMetrics: StepMonitoringConfiguration

    @RelaxedMockK
    private lateinit var redisToJavaConverter: RedisToJavaConverter

    @RelaxedMockK
    private lateinit var ioCoroutineScope: CoroutineScope

    @Test
    override fun `should not support unexpected spec`() {
        assertThat(converter.support(relaxedMockk()))
            .isFalse()
    }

    @Test
    override fun `should support expected spec`() {
        assertThat(converter.support(relaxedMockk<LettucePollStepSpecificationImpl<*>>()))
            .isTrue()

    }

    @ExperimentalCoroutinesApi
    @Test
    @Suppress("UNCHECK_CAST")
    fun `should convert with name`() {
        // given
        val spec = LettucePollStepSpecificationImpl<Any>(RedisLettuceScanMethod.SCAN)
        spec.also {
            it.name = "redis-lettuce-poll-step"
            it.keyOrPattern = "test"
            it.connection {
                nodes = listOf("localhost:6379")
            }
            it.monitoringConfig = lettucePollMetrics
        }
        val creationContext = StepCreationContextImpl(scenarioSpecification, directedAcyclicGraph, spec)

        // when
        runBlocking {
            @Suppress("UNCHECKED_CAST")
            converter.convert<Unit, Map<String, *>>(
                creationContext as StepCreationContext<LettucePollStepSpecificationImpl<*>>
            )
        }

        // then
        creationContext.createdStep!!.let {
            assertThat(it).isInstanceOf(IterativeDatasourceStep::class).all {
                prop("name").isNotNull().isEqualTo("redis-lettuce-poll-step")
                prop("reader").isNotNull().isInstanceOf(LettuceIterativeReader::class).all {
                    prop("ioCoroutineScope").isSameAs(ioCoroutineScope)
                    prop("connectionFactory").isNotNull()
                    prop("pattern").isEqualTo("test")
                    prop("pollDelay").isEqualTo(Duration.ofSeconds(10))
                    prop("lettuceScanner").isNotNull().isInstanceOf(LettuceKeysScanner::class)
                    prop("resultsChannelFactory").isNotNull()
                }
                prop("processor").isNotNull().isInstanceOf(NoopDatasourceObjectProcessor::class)
                prop("converter").isNotNull().isInstanceOf(PollResultSetBatchConverter::class)
            }
        }

        val channelFactory = creationContext.createdStep!!
            .getProperty<LettuceIterativeReader>("reader")
            .getProperty<() -> Channel<Any>>("resultsChannelFactory")
        val createdChannel = channelFactory()
        assertThat(createdChannel).all {
            transform { it.isEmpty }.isTrue()
            transform { it.isClosedForReceive }.isFalse()
            transform { it.isClosedForSend }.isFalse()
        }
    }

    @Test
    @Suppress("UNCHECK_CAST")
    fun `should convert without name`() {
        // given
        val spec = LettucePollStepSpecificationImpl<Any>(RedisLettuceScanMethod.SCAN)
        spec.also {
            it.keyOrPattern = "test"
            it.connection {
                nodes = listOf("localhost:6379")
            }
            it.monitoringConfig = lettucePollMetrics
        }
        val creationContext = StepCreationContextImpl(scenarioSpecification, directedAcyclicGraph, spec)

        // when
        runBlocking {
            @Suppress("UNCHECKED_CAST")
            converter.convert<Unit, Map<String, *>>(
                creationContext as StepCreationContext<LettucePollStepSpecificationImpl<*>>
            )
        }

        // then
        creationContext.createdStep!!.let { it ->
            assertThat(it).isInstanceOf(IterativeDatasourceStep::class).all {
                prop("name").isEqualTo("")
                prop("reader").isNotNull().isInstanceOf(LettuceIterativeReader::class).all {
                    prop("ioCoroutineScope").isSameAs(ioCoroutineScope)
                    prop("connectionFactory").isNotNull()
                    prop("pattern").isEqualTo("test")
                    prop("pollDelay").isEqualTo(Duration.ofSeconds(10))
                    prop("lettuceScanner").isNotNull().isInstanceOf(LettuceKeysScanner::class)
                    prop("resultsChannelFactory").isNotNull()
                }
                prop("processor").isNotNull().isInstanceOf(NoopDatasourceObjectProcessor::class)
                prop("converter").isNotNull().isInstanceOf(PollResultSetBatchConverter::class)
            }
        }
    }

}
