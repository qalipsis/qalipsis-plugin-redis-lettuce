/*
 * Copyright 2022 AERIS IT Solutions GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package io.qalipsis.plugins.redis.lettuce.save

import assertk.all
import assertk.assertThat
import assertk.assertions.hasSize
import assertk.assertions.isEqualTo
import assertk.assertions.isFalse
import assertk.assertions.isInstanceOf
import assertk.assertions.isNotNull
import assertk.assertions.isTrue
import assertk.assertions.prop
import io.aerisconsulting.catadioptre.getProperty
import io.qalipsis.api.context.StepContext
import io.qalipsis.api.steps.DummyStepSpecification
import io.qalipsis.api.steps.StepMonitoringConfiguration
import io.qalipsis.plugins.redis.lettuce.configuration.RedisConnectionConfiguration
import io.qalipsis.plugins.redis.lettuce.configuration.RedisConnectionType
import io.qalipsis.plugins.redis.lettuce.redisLettuce
import io.qalipsis.plugins.redis.lettuce.save.records.HashRecord
import io.qalipsis.plugins.redis.lettuce.save.records.SortedRecord
import io.qalipsis.plugins.redis.lettuce.streams.producer.LettuceStreamsProduceRecord
import io.qalipsis.test.coroutines.TestDispatcherProvider
import io.qalipsis.test.mockk.relaxedMockk
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.RegisterExtension

internal class LettuceSaveStepSpecificationImplTest {

    @JvmField
    @RegisterExtension
    val testDispatcherProvider = TestDispatcherProvider()

    @Test
    internal fun `should add minimal specification to the scenario with default values`() =
        testDispatcherProvider.runTest {
            val previousStep = DummyStepSpecification()
            previousStep.redisLettuce().save {
                name = "my-step"
                records { _, _ -> listOf(HashRecord("test", mapOf("test" to "test"))) }
            }

            assertThat(previousStep.nextSteps[0]).isInstanceOf(LettuceSaveStepSpecificationImpl::class)
                .all {
                    prop(LettuceSaveStepSpecificationImpl<*>::name).isEqualTo("my-step")
                    prop(LettuceSaveStepSpecificationImpl<*>::recordsFactory).isNotNull()
                prop(LettuceSaveStepSpecificationImpl<*>::connectionConfiguration).all {
                    prop(RedisConnectionConfiguration::nodes).isEqualTo(listOf("localhost:6379"))
                    prop(RedisConnectionConfiguration::database).isEqualTo(0)
                    prop(RedisConnectionConfiguration::authPassword).isEqualTo("")
                    prop(RedisConnectionConfiguration::redisConnectionType).isEqualTo(RedisConnectionType.SINGLE)
                    prop(RedisConnectionConfiguration::authUser).isEqualTo("")
                    prop(RedisConnectionConfiguration::masterId).isEqualTo("")
                }

                prop(LettuceSaveStepSpecificationImpl<*>::monitoringConfig).all {
                    prop(StepMonitoringConfiguration::events).isFalse()
                    prop(StepMonitoringConfiguration::meters).isFalse()
                }
            }

        val recordsFactory = previousStep.nextSteps[0].getProperty<suspend (ctx: StepContext<*, *>, input: Int) ->
        List<LettuceSaveRecord<*>>>("recordsFactory")
        assertThat(recordsFactory(relaxedMockk(), relaxedMockk())).isEqualTo(
            listOf(
               HashRecord(
                    "test",
                    mapOf("test" to "test")
                )
            )
        )
    }

    @Test
    internal fun `should add a complete specification to the scenario`() = testDispatcherProvider.runTest {
        val previousStep = DummyStepSpecification()
        previousStep.redisLettuce().save {
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
                    HashRecord("test", mapOf("test" to "test")),
                    SortedRecord("test", 2.0 to "test1")
                )
            }
        }

        assertThat(previousStep.nextSteps[0]).isInstanceOf(LettuceSaveStepSpecificationImpl::class)
            .all {
                prop(LettuceSaveStepSpecificationImpl<*>::name).isEqualTo("my-step-complete")
                prop(LettuceSaveStepSpecificationImpl<*>::recordsFactory).isNotNull()
                prop(LettuceSaveStepSpecificationImpl<*>::connectionConfiguration).all {
                    prop(RedisConnectionConfiguration::nodes).isEqualTo(listOf("localhost:6379"))
                    prop(RedisConnectionConfiguration::database).isEqualTo(1)
                    prop(RedisConnectionConfiguration::authPassword).isEqualTo("root")
                    prop(RedisConnectionConfiguration::redisConnectionType).isEqualTo(RedisConnectionType.CLUSTER)
                    prop(RedisConnectionConfiguration::authUser).isEqualTo("default")
                    prop(RedisConnectionConfiguration::masterId).isEqualTo("mymaster")
                }

                prop(LettuceSaveStepSpecificationImpl<*>::monitoringConfig).all {
                    prop(StepMonitoringConfiguration::events).isTrue()
                    prop(StepMonitoringConfiguration::meters).isTrue()
                }
            }

        val recordsFactory = previousStep.nextSteps[0].getProperty<suspend (ctx: StepContext<*, *>, input: Int) ->
        List<LettuceStreamsProduceRecord>>("recordsFactory")
        assertThat(recordsFactory(relaxedMockk(), relaxedMockk())).hasSize(2)
    }
}