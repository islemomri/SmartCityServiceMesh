
package org.example

import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking

fun main() = runBlocking {
    // 1. Create registry
    val registry = ServiceRegistry(this)

    // 2. Register all primary services and backups
    registry.register(CityService("traffic-01", "Traffic Service"))
    registry.register(CityService("traffic-backup-01", "Traffic Backup Service"))
    registry.register(CityService("environment-01", "Environment Service"))
    registry.register(CityService("environment-backup-01", "Environment Backup Service"))
    registry.register(CityService("lighting-01", "Smart Lighting Service"))
    registry.register(CityService("lighting-backup-01", "Lighting Backup Service"))

    // 3. Start health monitor
    val monitor = HealthMonitor(
        registry = registry,
        checkIntervalMillis = 1000L,
        timeoutMillis = 3000L
    )
    monitor.start(this)

    // 4. Create actors
    val trafficActor = ServiceActor("Traffic Service", this)
    val trafficBackupActor = ServiceActor("Traffic Backup Service", this)
    val environmentActor = ServiceActor("Environment Service", this)
    val environmentBackupActor = ServiceActor("Environment Backup Service", this)
    val lightingActor = ServiceActor("Smart Lighting Service", this)
    val lightingBackupActor = ServiceActor("Lighting Backup Service", this)

    // 5. Start actors
    trafficActor.start()
    trafficBackupActor.start()
    environmentActor.start()
    environmentBackupActor.start()
    lightingActor.start()
    lightingBackupActor.start()

    // 6. Connect services to actors
    val actors = mapOf(
        "traffic-01" to trafficActor,
        "traffic-backup-01" to trafficBackupActor,
        "environment-01" to environmentActor,
        "environment-backup-01" to environmentBackupActor,
        "lighting-01" to lightingActor,
        "lighting-backup-01" to lightingBackupActor
    )

    // 7. Configure backups
    val router = MessageRouter(
        registry = registry,
        actors = actors,
        backupServices = mapOf(
            "traffic-01" to "traffic-backup-01",
            "environment-01" to "environment-backup-01",
            "lighting-01" to "lighting-backup-01"
        )
    )

    // 8. Control primary heartbeats
    var trafficAlive = true
    var environmentAlive = true
    var lightingAlive = true

    // 9. Send heartbeats continuously.
    // Backups remain alive even when primary services fail.
    val heartbeatJob = launch {
        while (isActive) {
            if (trafficAlive) {
                monitor.receiveHeartbeat("traffic-01")
            }
            monitor.receiveHeartbeat("traffic-backup-01")

            if (environmentAlive) {
                monitor.receiveHeartbeat("environment-01")
            }
            monitor.receiveHeartbeat("environment-backup-01")

            if (lightingAlive) {
                monitor.receiveHeartbeat("lighting-01")
            }
            monitor.receiveHeartbeat("lighting-backup-01")

            delay(500L)
        }
    }

    suspend fun waitForStatus(
        serviceId: String,
        expected: ServiceStatus,
        timeoutMs: Long = 7000L
    ): Boolean {
        val start = System.nanoTime()

        while ((System.nanoTime() - start) / 1_000_000L < timeoutMs) {
            if (registry.getService(serviceId)?.status == expected) {
                return true
            }
            delay(50L)
        }

        return registry.getService(serviceId)?.status == expected
    }

    try {
        // Initial heartbeats
        println("\n=== Initial Heartbeats ===")
        delay(1200L)
        registry.printServices()

        // 10. Normal traffic routing
        println("\n=== Routing Test: Traffic ===")
        val normalTraffic = router.route(
            CityMessage(
                destinationServiceId = "traffic-01",
                content = "Request current traffic data"
            )
        )
        println("NORMAL TRAFFIC TEST: ${if (normalTraffic) "PASS" else "FAIL"}")

        // 11. Lighting failure
        println("\n=== Simulating Lighting Failure ===")
        val lightingFailureStart = System.nanoTime()
        lightingAlive = false

        val lightingDetected = waitForStatus(
            "lighting-01",
            ServiceStatus.UNREACHABLE
        )
        val lightingDetectionMs =
            (System.nanoTime() - lightingFailureStart) / 1_000_000L

        println("LIGHTING FAILURE DETECTION: ${lightingDetectionMs} ms")
        println("LIGHTING DETECTION TEST: ${if (lightingDetected) "PASS" else "FAIL"}")
        registry.printServices()

        // 12. Lighting rerouting
        println("\n=== Routing Test: Lighting Failure ===")
        val lightingBackupAvailable =
            registry.getService("lighting-backup-01")?.status ==
                    ServiceStatus.RUNNING

        val lightingRerouted = lightingDetected && lightingBackupAvailable &&
                router.route(
                    CityMessage(
                        destinationServiceId = "lighting-01",
                        content = "Update street lighting"
                    )
                )

        println("LIGHTING REROUTING TEST: ${if (lightingRerouted) "PASS" else "FAIL"}")

        // 13. Lighting recovery
        println("\n=== Simulating Lighting Recovery ===")
        lightingAlive = true

        val lightingRecovered = waitForStatus(
            "lighting-01",
            ServiceStatus.RUNNING
        )
        println("LIGHTING RECOVERY TEST: ${if (lightingRecovered) "PASS" else "FAIL"}")

        // 14. Routing after lighting recovery
        println("\n=== Routing Test: Lighting Recovery ===")
        val lightingNormal = router.route(
            CityMessage(
                destinationServiceId = "lighting-01",
                content = "Update street lighting after recovery"
            )
        )
        println("LIGHTING RECOVERY ROUTING: ${if (lightingNormal) "PASS" else "FAIL"}")

        // 15. Environment failure
        println("\n=== Simulating Environment Failure ===")
        val environmentFailureStart = System.nanoTime()
        environmentAlive = false

        val environmentDetected = waitForStatus(
            "environment-01",
            ServiceStatus.UNREACHABLE
        )
        val environmentDetectionMs =
            (System.nanoTime() - environmentFailureStart) / 1_000_000L

        println("ENVIRONMENT FAILURE DETECTION: ${environmentDetectionMs} ms")
        println(
            "ENVIRONMENT DETECTION TEST: " +
                    if (environmentDetected) "PASS" else "FAIL"
        )
        registry.printServices()

        // 16. Environment rerouting
        println("\n=== Routing Test: Environment Failure ===")
        val environmentBackupAvailable =
            registry.getService("environment-backup-01")?.status ==
                    ServiceStatus.RUNNING

        val environmentRerouted =
            environmentDetected && environmentBackupAvailable &&
                    router.route(
                        CityMessage(
                            destinationServiceId = "environment-01",
                            content = "Request environmental sensor data"
                        )
                    )

        println(
            "ENVIRONMENT REROUTING TEST: " +
                    if (environmentRerouted) "PASS" else "FAIL"
        )

        // 17. Environment recovery
        println("\n=== Simulating Environment Recovery ===")
        environmentAlive = true

        val environmentRecovered = waitForStatus(
            "environment-01",
            ServiceStatus.RUNNING
        )
        println(
            "ENVIRONMENT RECOVERY TEST: " +
                    if (environmentRecovered) "PASS" else "FAIL"
        )

        // 18. Routing after environment recovery
        println("\n=== Routing Test: Environment Recovery ===")
        val environmentNormal = router.route(
            CityMessage(
                destinationServiceId = "environment-01",
                content = "Request environmental data after recovery"
            )
        )
        println(
            "ENVIRONMENT RECOVERY ROUTING: " +
                    if (environmentNormal) "PASS" else "FAIL"
        )

        // 19. Traffic failure and detection-time measurement
        println("\n=== Simulating Traffic Failure ===")
        val detectionStart = System.nanoTime()
        trafficAlive = false

        val trafficDetected = waitForStatus(
            "traffic-01",
            ServiceStatus.UNREACHABLE
        )
        val detectionTimeMs =
            (System.nanoTime() - detectionStart) / 1_000_000L

        println("FAILURE DETECTION TIME: ${detectionTimeMs} ms")
        println("FAILURE DETECTION TEST: ${if (trafficDetected) "PASS" else "FAIL"}")
        registry.printServices()

        // 20. Traffic automatic rerouting
        println("\n=== Health Monitor Rerouting Test ===")
        val trafficBackupAvailable =
            registry.getService("traffic-backup-01")?.status ==
                    ServiceStatus.RUNNING

        val reroutingStart = System.nanoTime()
        val reroutingPassed = trafficDetected && trafficBackupAvailable &&
                router.route(
                    CityMessage(
                        destinationServiceId = "traffic-01",
                        content = "Request current traffic data after primary failure"
                    )
                )

        val reroutingTimeMs =
            (System.nanoTime() - reroutingStart) / 1_000_000L

        println("AUTOMATIC REROUTING TIME: ${reroutingTimeMs} ms")
        println(
            "HEALTH-MONITOR REROUTING TEST: " +
                    if (reroutingPassed) "PASS" else "FAIL"
        )

        // 21. Restore Traffic
        println("\n=== Restoring Traffic ===")
        trafficAlive = true

        val trafficRecovered = waitForStatus(
            "traffic-01",
            ServiceStatus.RUNNING
        )
        println("TRAFFIC RECOVERY TEST: ${if (trafficRecovered) "PASS" else "FAIL"}")

        // 22. Message Storm Test
        println("\n=== Message Storm Test ===")
        var successfulMessages = 0

        for (i in 1..100) {
            val accepted = router.route(
                CityMessage(
                    destinationServiceId = "traffic-01",
                    content = "Storm message $i"
                )
            )
            if (accepted) successfulMessages++
        }

        println("Messages accepted by router: $successfulMessages/100")
        println(
            "MESSAGE STORM TEST: " +
                    if (successfulMessages == 100) "PASS" else "FAIL"
        )

        // 23. Parallel Backpressure Test
        println("\n=== Parallel Backpressure Test ===")
        val parallelStart = System.nanoTime()

        val parallelResults = coroutineScope {
            (1..50).map { i ->
                async {
                    router.route(
                        CityMessage(
                            destinationServiceId = "traffic-01",
                            content = "Parallel message $i"
                        )
                    )
                }
            }.awaitAll()
        }

        val acceptedCount = parallelResults.count { it }
        val parallelDuration =
            (System.nanoTime() - parallelStart) / 1_000_000L

        println("Parallel messages accepted: $acceptedCount/50")
        println("Parallel test duration: $parallelDuration ms")
        println(
            "PARALLEL TEST: " +
                    if (acceptedCount == 50) "PASS" else "FAIL"
        )

        registry.printServices()
    } finally {
        heartbeatJob.cancelAndJoin()

        monitor.stop()

        trafficActor.stop()
        trafficBackupActor.stop()
        environmentActor.stop()
        environmentBackupActor.stop()
        lightingActor.stop()
        lightingBackupActor.stop()

        registry.stop()
    }

    println("\n=== Test Completed ===")
}
