
package org.example

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.coroutineScope

private data class TestResult(
    val accepted: Int,
    val rejected: Int
)

private suspend fun runCampaignTest(
    testName: String,
    silentServices: Set<String>,
    stormMessages: Int = 0
) = coroutineScope {
    println("\n========================================")
    println("TEST: $testName")
    println("========================================")

    val registry = ServiceRegistry(this)

    val serviceNames = mapOf(
        "traffic-01" to "Traffic Service",
        "environment-01" to "Environment Service",
        "lighting-01" to "Smart Lighting Service"
    )

    for ((id, name) in serviceNames) {
        registry.register(CityService(id, name))
    }

    val actors = serviceNames.mapValues { (_, name) ->
        ServiceActor(name, this).also { it.start() }
    }

    val monitor = HealthMonitor(
        registry = registry,
        checkIntervalMillis = 200L,
        timeoutMillis = 800L
    )
    monitor.start(this)

    // Keep heartbeats running only for services not declared silent.
    val heartbeatJobs = serviceNames.keys
        .filterNot { it in silentServices }
        .map { id ->
            launch {
                while (true) {
                    monitor.receiveHeartbeat(id)
                    delay(150L)
                }
            }
        }

    val router = MessageRouter(registry, actors)

    try {
        // Allow services to establish their initial heartbeat state.
        delay(350L)

        println("\n--- Status BEFORE fault window ---")
        registry.printServices()

        // Silent services stop sending heartbeats from the start.
        // Wait beyond the configured failure timeout.
        delay(1100L)

        println("\n--- Status AFTER fault detection ---")
        registry.printServices()

        val result = if (stormMessages > 0) {
            val outcomes = coroutineScope {
                (1..stormMessages).map { i ->
                    async {
                        router.route(
                            CityMessage(
                                destinationServiceId = "traffic-01",
                                content = "Storm message $i"
                            )
                        )
                    }
                }.awaitAll()
            }

            TestResult(
                accepted = outcomes.count { it },
                rejected = outcomes.count { !it }
            )
        } else {
            // Probe each service once to count routable vs rejected messages.
            val outcomes = serviceNames.keys.map { id ->
                router.route(
                    CityMessage(
                        destinationServiceId = id,
                        content = "Resilience probe for $id"
                    )
                )
            }

            TestResult(
                accepted = outcomes.count { it },
                rejected = outcomes.count { !it }
            )
        }

        println("\n--- Test metrics ---")
        println("Messages accepted: ${result.accepted}")
        println("Messages rejected: ${result.rejected}")

        val statuses = registry.getAllServices()
        val running = statuses.count {
            it.status == ServiceStatus.RUNNING
        }
        val unreachable = statuses.count {
            it.status == ServiceStatus.UNREACHABLE
        }

        val unexpectedFailures = statuses.filter {
            it.status == ServiceStatus.UNREACHABLE &&
                    it.id !in silentServices
        }

        val passed = unexpectedFailures.isEmpty()

        println("\n--- RESULTS ---")
        println("Accepted: ${result.accepted} | Rejected: ${result.rejected}")
        println("Running: $running/${statuses.size} | Unreachable: $unreachable")
        println("Unexpected failures: ${unexpectedFailures.size}")
        println("Result: ${if (passed) "PASS" else "FAIL"}")

    } finally {
        heartbeatJobs.forEach { it.cancelAndJoin() }
        monitor.stop()
        actors.values.forEach { it.stop() }
        registry.stop()
    }
}

fun main() = runBlocking {
    // Test 1: only lighting becomes silent.
    runCampaignTest(
        testName = "Single-node silence",
        silentServices = setOf("lighting-01")
    )

    // Test 2: traffic and lighting become silent simultaneously.
    runCampaignTest(
        testName = "Two-node failure",
        silentServices = setOf("traffic-01", "lighting-01")
    )

    // Test 3: no node failure; send a burst of messages.
    runCampaignTest(
        testName = "Message storm",
        silentServices = emptySet(),
        stormMessages = 100
    )

    println("\n=== RESILIENCE CAMPAIGN COMPLETED ===")
}
