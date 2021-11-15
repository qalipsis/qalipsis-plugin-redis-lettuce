package io.qalipsis.plugins.redis.lettuce

import io.qalipsis.api.annotations.Spec

@Spec
data class Monitoring internal constructor(
    var events: Boolean = false,
    var meters: Boolean = false
)