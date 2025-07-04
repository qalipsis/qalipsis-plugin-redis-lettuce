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

import io.qalipsis.api.annotations.Spec
import io.qalipsis.api.context.StepContext
import io.qalipsis.api.steps.AbstractStepSpecification
import io.qalipsis.api.steps.ConfigurableStepSpecification
import io.qalipsis.api.steps.StepMonitoringConfiguration
import io.qalipsis.api.steps.StepSpecification
import io.qalipsis.plugins.redis.lettuce.RedisLettuceStepSpecification
import io.qalipsis.plugins.redis.lettuce.configuration.RedisConnectionConfiguration
import io.qalipsis.plugins.redis.lettuce.configuration.RedisConnectionType.CLUSTER
import io.qalipsis.plugins.redis.lettuce.configuration.RedisConnectionType.SENTINEL
import io.qalipsis.plugins.redis.lettuce.configuration.RedisConnectionType.SINGLE

/**
 * Specification for a [LettuceStreamsProducerStep] to produce data onto a Redis stream.
 *
 * The output is a [LettuceStreamsProducerResult] that contains the output from previous step and metrics regarding this step.
 *
 * @author Gabriel Moraes
 */
@Spec
interface LettuceStreamsProducerStepSpecification<I> :
    StepSpecification<I, LettuceStreamsProducerResult<I>, LettuceStreamsProducerStepSpecification<I>>,
    ConfigurableStepSpecification<I, LettuceStreamsProducerResult<I>, LettuceStreamsProducerStepSpecification<I>> {

    /**
     * Configures the connection to the database.
     *
     * The connection type is specified in the [configBlock] by using either [SINGLE], [CLUSTER] or [SENTINEL].
     * It is also possible to connect to more than one node, this can be achieved by passing a more than one
     * parameter in the [List] of nodes for the [configBlock].
     *
     * For the [SENTINEL] connection type is required to pass a value for the masterId parameter.
     */
    fun connection(configBlock: RedisConnectionConfiguration.() -> Unit)

    /**
     * Defines the records to be published, it receives the context and the output from previous step that can be used
     * when defining the records.
     */
    fun records(recordsConfiguration: suspend (stepContext: StepContext<*, *>, input: I) -> List<LettuceStreamsProduceRecord>)

    /**
     * Configures the monitoring of the poll step.
     */
    fun monitoring(monitoringConfig: StepMonitoringConfiguration.() -> Unit)
}

/**
 * Specification to a Redis streams publisher, implementation of [LettuceStreamsProducerStepSpecification].
 *
 * @author Gabriel Moraes
 */
@Spec
internal class LettuceStreamsProducerStepSpecificationImpl<I> :
    AbstractStepSpecification<I, LettuceStreamsProducerResult<I>, LettuceStreamsProducerStepSpecification<I>>(),
    LettuceStreamsProducerStepSpecification<I> {

    internal var monitoringConfig = StepMonitoringConfiguration()

    internal var connection = RedisConnectionConfiguration()

    override fun connection(configBlock: RedisConnectionConfiguration.() -> Unit) {
        connection.configBlock()
    }

    override fun monitoring(monitoringConfig: StepMonitoringConfiguration.() -> Unit) {
        this.monitoringConfig.monitoringConfig()
    }

    internal var recordsFactory: (suspend (stepContext: StepContext<*, *>, input: I) ->
    List<LettuceStreamsProduceRecord>) = { _, _ -> emptyList() }

    override fun records(recordsConfiguration: suspend (stepContext: StepContext<*, *>, input: I) -> List<LettuceStreamsProduceRecord>) {
        this.recordsFactory = recordsConfiguration
    }

}


/**
 * Creates a step to push data onto streams of a Redis broker and forwards the input to the next step.
 *
 * You can learn more on [lettuce website](https://lettuce.io/docs/getting-started.html).
 *
 * @author Gabriel Moraes
 */
fun <I> RedisLettuceStepSpecification<*, I, *>.streamsProduce(
    configurationBlock: LettuceStreamsProducerStepSpecification<I>.() -> Unit
): LettuceStreamsProducerStepSpecification<I> {
    val step = LettuceStreamsProducerStepSpecificationImpl<I>()
    step.configurationBlock()

    this.add(step)
    return step
}