/*
 * QALIPSIS
 * Copyright (C) 2025 AERIS IT Solutions GmbH
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package io.qalipsis.plugins.redis.lettuce.streams.producer

import assertk.all
import assertk.assertThat
import assertk.assertions.*
import io.aerisconsulting.catadioptre.getProperty
import io.qalipsis.api.context.StepContext
import io.qalipsis.api.steps.DummyStepSpecification
import io.qalipsis.api.steps.StepMonitoringConfiguration
import io.qalipsis.plugins.redis.lettuce.configuration.RedisConnectionConfiguration
import io.qalipsis.plugins.redis.lettuce.configuration.RedisConnectionType
import io.qalipsis.plugins.redis.lettuce.redisLettuce
import io.qalipsis.test.coroutines.TestDispatcherProvider
import io.qalipsis.test.mockk.relaxedMockk
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.RegisterExtension

internal class LettuceStreamsProducerStepSpecificationImplTest {

    @JvmField
    @RegisterExtension
    val testDispatcherProvider = TestDispatcherProvider()

    @Test
    internal fun `should add minimal specification to the scenario with default values`() =
        testDispatcherProvider.runTest {
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

                prop(LettuceStreamsProducerStepSpecificationImpl<*>::monitoringConfig).all {
                    prop(StepMonitoringConfiguration::events).isFalse()
                    prop(StepMonitoringConfiguration::meters).isFalse()
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
    internal fun `should add a complete specification to the scenario as broadcast`() = testDispatcherProvider.runTest {
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

                prop(LettuceStreamsProducerStepSpecificationImpl<*>::monitoringConfig).all {
                    prop(StepMonitoringConfiguration::events).isTrue()
                    prop(StepMonitoringConfiguration::meters).isTrue()
                }
            }

        val recordsBuilder = previousStep.nextSteps[0].getProperty<suspend (ctx: StepContext<*, *>, input: Int) ->
        List<LettuceStreamsProduceRecord>>("recordsFactory")
        assertThat(recordsBuilder(relaxedMockk(), relaxedMockk())).hasSize(2)
    }

}