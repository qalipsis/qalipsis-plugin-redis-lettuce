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

package io.qalipsis.plugins.redis.lettuce.streams.producer

import io.micrometer.core.instrument.MeterRegistry
import io.qalipsis.api.Executors
import io.qalipsis.api.annotations.StepConverter
import io.qalipsis.api.events.EventsLogger
import io.qalipsis.api.steps.StepCreationContext
import io.qalipsis.api.steps.StepSpecification
import io.qalipsis.api.steps.StepSpecificationConverter
import io.qalipsis.api.sync.asSuspended
import io.qalipsis.plugins.redis.lettuce.configuration.RedisStatefulConnectionFactory
import jakarta.inject.Named
import java.time.Duration
import kotlin.coroutines.CoroutineContext

/**
 * [StepSpecificationConverter] from [LettuceStreamsProducerStepSpecificationImpl] to [LettuceStreamsProducerStep].
 *
 * @author Gabriel Moraes
 */
@StepConverter
internal class LettuceStreamsProducerStepSpecificationConverter(
    private val eventsLogger: EventsLogger,
    private val meterRegistry: MeterRegistry,
    @Named(Executors.IO_EXECUTOR_NAME) private val ioCoroutineContext: CoroutineContext
) : StepSpecificationConverter<LettuceStreamsProducerStepSpecificationImpl<*>> {

    override fun support(stepSpecification: StepSpecification<*, *, *>): Boolean {
        return stepSpecification is LettuceStreamsProducerStepSpecificationImpl<*>
    }

    override suspend fun <I, O> convert(creationContext: StepCreationContext<LettuceStreamsProducerStepSpecificationImpl<*>>) {
        val spec = creationContext.stepSpecification
        val connectionFactory = suspend {
            RedisStatefulConnectionFactory(spec.connection).create().asSuspended().get(
                CONNECTION_TIMEOUT
            )
        }

        val step = LettuceStreamsProducerStep(
            id = spec.name,
            retryPolicy = spec.retryPolicy,
            ioCoroutineContext = ioCoroutineContext,
            connectionFactory = connectionFactory,
            recordsFactory = spec.recordsFactory,
            eventsLogger = eventsLogger.takeIf { spec.monitoringConfig.events },
            meterRegistry = meterRegistry.takeIf { spec.monitoringConfig.meters }
        )

        creationContext.createdStep(step)
    }

    companion object {
        private val CONNECTION_TIMEOUT = Duration.ofSeconds(10)
    }
}