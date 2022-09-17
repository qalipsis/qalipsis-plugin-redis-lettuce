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

package io.qalipsis.plugins.redis.lettuce.poll

import io.qalipsis.plugins.redis.lettuce.RedisRecord

/**
 * Wrapper for the result of poll in Redis.
 *
 * @author Svetlana Paliashchuk
 *
 * @property records list of Redis records.
 * @property meters metrics of the poll step.
 *
 */
data class LettucePollResult<V>(
    val records: List<RedisRecord<V>>,
    val meters: LettucePollMeters
) : Iterable<RedisRecord<V>> {

    override fun iterator(): Iterator<RedisRecord<V>> {
        return records.iterator()
    }
}
