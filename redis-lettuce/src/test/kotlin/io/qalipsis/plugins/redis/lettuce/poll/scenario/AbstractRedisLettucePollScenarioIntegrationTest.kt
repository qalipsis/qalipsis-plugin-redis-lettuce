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

package io.qalipsis.plugins.redis.lettuce.poll.scenario

import assertk.all
import assertk.assertThat
import assertk.assertions.containsExactlyInAnyOrder
import assertk.assertions.hasSize
import io.lettuce.core.ScoredValue
import io.qalipsis.plugins.redis.lettuce.AbstractRedisIntegrationTest
import io.qalipsis.runtime.test.QalipsisTestRunner
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.testcontainers.containers.GenericContainer
import org.testcontainers.junit.jupiter.Testcontainers

@Testcontainers
internal abstract class AbstractRedisLettucePollScenarioIntegrationTest(private val container: GenericContainer<Nothing>) :
    AbstractRedisIntegrationTest(container) {


    @BeforeEach
    override fun setUp() {
        super.setUp()
        PollScenario.dbNodes = listOf("${container.host}:${container.getMappedPort(REDIS_PORT)}")
    }

    @Test
    @Timeout(20)
    fun `should be able to poll redis scan`() {
        PollScenario.resetReceivedMessages()

        insertKeyValue("scan-test", "Car")
        insertKeyValue("scan-test1", "Truck")
        insertKeyValue("scan-test2", "Car")

        val exitCode = QalipsisTestRunner.withScenarios("poll-scan").execute()

        Assertions.assertEquals(0, exitCode)

        assertThat(PollScenario.receivedMessages).all {
            hasSize(3)
            containsExactlyInAnyOrder(
                "scan-test",
                "scan-test1",
                "scan-test2"
            )
        }
    }

    @Test
    @Timeout(20)
    fun `should be able to poll redis sscan`() {
        PollScenario.resetReceivedMessages()
        insertSet("test", "alice", "bob", "david")
        insertSet("testout", "bob", "alice", "charles", "david")

        val exitCode = QalipsisTestRunner.withScenarios("poll-sscan").execute()

        Assertions.assertEquals(0, exitCode)

        assertThat(PollScenario.receivedMessages).all {
            hasSize(3)
            containsExactlyInAnyOrder(
                "alice",
                "bob",
                "david",
            )
        }
    }

    @Test
    @Timeout(20)
    fun `should be able to poll redis hscan`() {
        PollScenario.resetReceivedMessages()
        insertHash("hscan-test", mapOf("alice" to "red", "cris" to "blue"))

        val exitCode = QalipsisTestRunner.withScenarios("poll-hscan").execute()

        Assertions.assertEquals(0, exitCode)

        assertThat(PollScenario.receivedMessages).all {
            hasSize(2)
            containsExactlyInAnyOrder(
                "alice red",
                "cris blue",
            )
        }
    }

    @Test
    @Timeout(20)
    fun `should be able to poll redis zscan`() {
        PollScenario.resetReceivedMessages()
        insertSortedSet(
            "zscan-test",
            ScoredValue.just(1.0, "alice"),
            ScoredValue.just(2.0, "peter"),
            ScoredValue.just(3.0, "patrick"),
        )

        val exitCode = QalipsisTestRunner.withScenarios("poll-zscan").execute()

        Assertions.assertEquals(0, exitCode)

        assertThat(PollScenario.receivedMessages).all {
            hasSize(3)
            containsExactlyInAnyOrder(
                "1.0 alice",
                "2.0 peter",
                "3.0 patrick",
            )
        }
    }

}
