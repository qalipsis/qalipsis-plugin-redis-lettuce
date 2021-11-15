package io.qalipsis.plugins.redis.lettuce.streams.producer

import assertk.all
import assertk.assertThat
import assertk.assertions.*
import io.aerisconsulting.catadioptre.getProperty
import io.qalipsis.api.context.StepContext
import io.qalipsis.api.steps.DummyStepSpecification
import io.qalipsis.plugins.redis.lettuce.Monitoring
import io.qalipsis.plugins.redis.lettuce.configuration.RedisConnectionConfiguration
import io.qalipsis.plugins.redis.lettuce.configuration.RedisConnectionType
import io.qalipsis.plugins.redis.lettuce.redisLettuce
import io.qalipsis.test.mockk.relaxedMockk
import kotlinx.coroutines.test.runBlockingTest
import org.junit.jupiter.api.Test

internal class LettuceStreamsProducerStepSpecificationImplTest {

    @Test
    internal fun `should add minimal specification to the scenario with default values`() = runBlockingTest {
        val previousStep = DummyStepSpecification()
        previousStep.redisLettuce().streamsProduce {
            name = "my-step"
            records { _, _ -> listOf(LettuceStreamsProduceRecord("test", mapOf("test" to "test"))) }
        }

        assertThat(previousStep.nextSteps[0]).isInstanceOf(LettuceStreamsProducerStepSpecificationImpl::class)
            .all {
                prop(LettuceStreamsProducerStepSpecificationImpl<*>::name).isEqualTo("my-step")
                prop(LettuceStreamsProducerStepSpecificationImpl<*>::recordsFactory).isNotNull()
                prop(LettuceStreamsProducerStepSpecificationImpl<*>::connection).all {
                    prop(RedisConnectionConfiguration::nodes).isEqualTo(listOf("localhost:6379"))
                    prop(RedisConnectionConfiguration::database).isEqualTo(0)
                    prop(RedisConnectionConfiguration::authPassword).isEqualTo("")
                    prop(RedisConnectionConfiguration::redisConnectionType).isEqualTo(RedisConnectionType.SINGLE)
                    prop(RedisConnectionConfiguration::authUser).isEqualTo("")
                    prop(RedisConnectionConfiguration::masterId).isEqualTo("")
                }

                prop(LettuceStreamsProducerStepSpecificationImpl<*>::monitoringConfiguration).all {
                    prop(Monitoring::events).isFalse()
                    prop(Monitoring::meters).isFalse()
                }
            }

        val recordsBuilder = previousStep.nextSteps[0].getProperty<suspend (ctx: StepContext<*, *>, input: Int) ->
        List<LettuceStreamsProduceRecord>>("recordsFactory")
        assertThat(recordsBuilder(relaxedMockk(), relaxedMockk())).isEqualTo(
            listOf(
                LettuceStreamsProduceRecord(
                    "test",
                    mapOf("test" to "test")
                )
            )
        )
    }

    @Test
    internal fun `should add a complete specification to the scenario as broadcast`() = runBlockingTest {
        val previousStep = DummyStepSpecification()
        previousStep.redisLettuce().streamsProduce {
            name = "my-step-complete"
            connection {
                nodes = listOf("localhost:6379")
                database = 1
                redisConnectionType = RedisConnectionType.CLUSTER
                authPassword = "root"
                authUser = "default"
                masterId = "mymaster"
            }
            monitoring {
                events = true
                meters = true
            }
            records { _, _ ->
                listOf(
                    LettuceStreamsProduceRecord("test", mapOf("test" to "test")),
                    LettuceStreamsProduceRecord("test", mapOf("test1" to "test1"))
                )
            }
        }

        assertThat(previousStep.nextSteps[0]).isInstanceOf(LettuceStreamsProducerStepSpecificationImpl::class)
            .all {
                prop(LettuceStreamsProducerStepSpecificationImpl<*>::name).isEqualTo("my-step-complete")
                prop(LettuceStreamsProducerStepSpecificationImpl<*>::recordsFactory).isNotNull()
                prop(LettuceStreamsProducerStepSpecificationImpl<*>::connection).all {
                    prop(RedisConnectionConfiguration::nodes).isEqualTo(listOf("localhost:6379"))
                    prop(RedisConnectionConfiguration::database).isEqualTo(1)
                    prop(RedisConnectionConfiguration::authPassword).isEqualTo("root")
                    prop(RedisConnectionConfiguration::redisConnectionType).isEqualTo(RedisConnectionType.CLUSTER)
                    prop(RedisConnectionConfiguration::authUser).isEqualTo("default")
                    prop(RedisConnectionConfiguration::masterId).isEqualTo("mymaster")
                }

                prop(LettuceStreamsProducerStepSpecificationImpl<*>::monitoringConfiguration).all {
                    prop(Monitoring::events).isTrue()
                    prop(Monitoring::meters).isTrue()
                }
            }

        val recordsBuilder = previousStep.nextSteps[0].getProperty<suspend (ctx: StepContext<*, *>, input: Int) ->
        List<LettuceStreamsProduceRecord>>("recordsFactory")
        assertThat(recordsBuilder(relaxedMockk(), relaxedMockk())).hasSize(2)
    }

}