package io.qalipsis.plugins.redis.lettuce.poll

import assertk.all
import assertk.assertThat
import assertk.assertions.isEqualTo
import assertk.assertions.isFalse
import assertk.assertions.isInstanceOf
import assertk.assertions.isTrue
import assertk.assertions.prop
import io.qalipsis.api.scenario.StepSpecificationRegistry
import io.qalipsis.api.scenario.scenario
import io.qalipsis.api.steps.SingletonConfiguration
import io.qalipsis.api.steps.SingletonType
import io.qalipsis.api.steps.StepMonitoringConfiguration
import io.qalipsis.plugins.redis.lettuce.configuration.RedisConnectionConfiguration
import io.qalipsis.plugins.redis.lettuce.configuration.RedisConnectionType
import io.qalipsis.plugins.redis.lettuce.redisLettuce
import org.junit.jupiter.api.Test
import java.time.Duration

/**
 *
 * @author Gabriel Moraes
 */
internal class LettucePollStepSpecificationImplTest {

    @Test
    internal fun `should add minimal specification to the scenario with default values`() {
        val scenario = scenario("my-scenario") as StepSpecificationRegistry
        scenario.redisLettuce().pollScan {
            name = "my-step"
            keyOrPattern("test")
        }

        assertThat(scenario.rootSteps.first()).isInstanceOf(LettucePollStepSpecificationImpl::class).all {
            prop(LettucePollStepSpecificationImpl<*>::name).isEqualTo("my-step")
            prop(LettucePollStepSpecificationImpl<*>::keyOrPattern).isEqualTo("test")
            prop(LettucePollStepSpecificationImpl<*>::connection).all {
                prop(RedisConnectionConfiguration::nodes).isEqualTo(listOf("localhost:6379"))
                prop(RedisConnectionConfiguration::database).isEqualTo(0)
                prop(RedisConnectionConfiguration::authPassword).isEqualTo("")
                prop(RedisConnectionConfiguration::redisConnectionType).isEqualTo(RedisConnectionType.SINGLE)
                prop(RedisConnectionConfiguration::authUser).isEqualTo("")
                prop(RedisConnectionConfiguration::masterId).isEqualTo("")
            }

            prop(LettucePollStepSpecificationImpl<*>::pollDelay).isEqualTo(
                Duration.ofSeconds(10)
            )
            prop(LettucePollStepSpecificationImpl<*>::monitoringConfig).all {
                prop(StepMonitoringConfiguration::events).isFalse()
                prop(StepMonitoringConfiguration::meters).isFalse()
            }
            prop(LettucePollStepSpecificationImpl<*>::singletonConfiguration).all {
                prop(SingletonConfiguration::type).isEqualTo(SingletonType.UNICAST)
                prop(SingletonConfiguration::bufferSize).isEqualTo(-1)
                prop(SingletonConfiguration::idleTimeout).isEqualTo(Duration.ZERO)
            }
            prop(LettucePollStepSpecificationImpl<*>::redisMethod).isEqualTo(RedisLettuceScanMethod.SCAN)
        }
    }

    @Test
    internal fun `should add a complete specification to the scenario as broadcast`() {
        val scenario = scenario("my-scenario") as StepSpecificationRegistry
        scenario.redisLettuce().pollScan {
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
            pollDelay(Duration.ofSeconds(2))
            keyOrPattern("test")
            broadcast(10, Duration.ofSeconds(10))
        }

        assertThat(scenario.rootSteps.first()).isInstanceOf(LettucePollStepSpecificationImpl::class).all {
            prop(LettucePollStepSpecificationImpl<*>::name).isEqualTo("my-step-complete")
            prop(LettucePollStepSpecificationImpl<*>::keyOrPattern).isEqualTo("test")
            prop(LettucePollStepSpecificationImpl<*>::connection).all {
                prop(RedisConnectionConfiguration::nodes).isEqualTo(listOf("localhost:6379"))
                prop(RedisConnectionConfiguration::database).isEqualTo(1)
                prop(RedisConnectionConfiguration::authPassword).isEqualTo("root")
                prop(RedisConnectionConfiguration::redisConnectionType).isEqualTo(RedisConnectionType.CLUSTER)
                prop(RedisConnectionConfiguration::authUser).isEqualTo("default")
                prop(RedisConnectionConfiguration::masterId).isEqualTo("mymaster")
            }

            prop(LettucePollStepSpecificationImpl<*>::pollDelay).isEqualTo(
                Duration.ofSeconds(2)
            )
            prop(LettucePollStepSpecificationImpl<*>::monitoringConfig).all {
                prop(StepMonitoringConfiguration::events).isTrue()
                prop(StepMonitoringConfiguration::meters).isTrue()
            }
            prop(LettucePollStepSpecificationImpl<*>::singletonConfiguration).all {
                prop(SingletonConfiguration::type).isEqualTo(SingletonType.BROADCAST)
                prop(SingletonConfiguration::bufferSize).isEqualTo(10)
                prop(SingletonConfiguration::idleTimeout).isEqualTo(Duration.ofSeconds(10))
            }
            prop(LettucePollStepSpecificationImpl<*>::redisMethod).isEqualTo(RedisLettuceScanMethod.SCAN)
        }
    }

    @Test
    internal fun `should validate redis sscan method specification`() {
        val scenario = scenario("my-scenario") as StepSpecificationRegistry
        scenario.redisLettuce().pollSscan {
            name = "my-step"
            keyOrPattern("test")
        }

        assertThat(scenario.rootSteps.first()).isInstanceOf(LettucePollStepSpecificationImpl::class).all {
            prop(LettucePollStepSpecificationImpl<*>::redisMethod).isEqualTo(RedisLettuceScanMethod.SSCAN)
        }
    }

    @Test
    internal fun `should validate redis hscan method specification`() {
        val scenario = scenario("my-scenario") as StepSpecificationRegistry
        scenario.redisLettuce().pollHscan {
            name = "my-step"
            keyOrPattern("test")
        }

        assertThat(scenario.rootSteps.first()).isInstanceOf(LettucePollStepSpecificationImpl::class).all {
            prop(LettucePollStepSpecificationImpl<*>::redisMethod).isEqualTo(RedisLettuceScanMethod.HSCAN)
        }
    }

    @Test
    internal fun `should validate redis zscan method specification`() {
        val scenario = scenario("my-scenario") as StepSpecificationRegistry
        scenario.redisLettuce().pollZscan {
            name = "my-step"
            keyOrPattern("test")
        }

        assertThat(scenario.rootSteps.first()).isInstanceOf(LettucePollStepSpecificationImpl::class).all {
            prop(LettucePollStepSpecificationImpl<*>::redisMethod).isEqualTo(RedisLettuceScanMethod.ZSCAN)
        }
    }

}
