
package org.example

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import org.eclipse.californium.core.CoapClient
import org.eclipse.californium.core.coap.CoAP
import org.eclipse.californium.core.coap.MediaTypeRegistry

fun main() = runBlocking {
    val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    val registry = ServiceRegistry(scope)

    val trafficActor = ServiceActor("Traffic Service", scope)
    val environmentActor = ServiceActor("Environment Service", scope)
    val lightingActor = ServiceActor("Smart Lighting Service", scope)
    val trafficBackupActor =
        ServiceActor("Traffic Backup Service", scope)
    val actors = mapOf(
        "traffic-01" to trafficActor,
        "traffic-backup-01" to trafficBackupActor,
        "environment-01" to environmentActor,
        "lighting-01" to lightingActor
    )

    val router = MessageRouter(
        registry = registry,
        actors = actors,
        backupServices = mapOf(
            "traffic-01" to "traffic-backup-01"
        )
    )
    trafficActor.start()
    trafficBackupActor.start()
    environmentActor.start()
    lightingActor.start()

    var server: CoapAdapter? = null

    var getPassed = 0
    var postPassed = 0
    var errorPassed = 0
    var timeoutPassed = 0

    try {
        // Register services
        registry.register(
            CityService(
                id = "traffic-01",
                name = "Traffic Service"
            )
        )
        registry.register(
            CityService(
                id = "traffic-backup-01",
                name = "Traffic Backup Service"
            )
        )
        registry.register(
            CityService(
                id = "environment-01",
                name = "Environment Service"
            )
        )

        registry.register(
            CityService(
                id = "lighting-01",
                name = "Smart Lighting Service"
            )
        )
// STEP 0: Automatic rerouting test
        println("\nTesting automatic rerouting")

        registry.updateStatus(
            id = "traffic-01",
            status = ServiceStatus.UNREACHABLE
        )

        val reroutingResult = router.route(
            CityMessage(
                destinationServiceId = "traffic-01",
                content = """{"event":"traffic-failover-test","vehicleCount":10}"""
            )
        )

        println(
            "AUTOMATIC REROUTING TEST: " +
                    if (reroutingResult) "PASS" else "FAIL"
        )

// Restore the primary for the existing CoAP tests.
        registry.updateStatus(
            id = "traffic-01",
            status = ServiceStatus.RUNNING
        )
        // Start CoAP server
        server = CoapAdapter(
            registry = registry,
            router = router,
            serviceId = "traffic-01",
            port = 5683
        )
        server.start()

        // STEP 1: GET tests
        val getTests = listOf(
            "/status" to "traffic-01",
            "/traffic" to "traffic-01",
            "/environment" to "environment-01",
            "/lighting" to "lighting-01"
        )

        for ((path, expectedService) in getTests) {
            println("\nTesting GET $path")
            val client = CoapClient("coap://127.0.0.1:5683$path")

            try {
                val response = client.get()
                val body = response?.responseText ?: ""

                val passed =
                    response?.code == CoAP.ResponseCode.CONTENT &&
                            body.contains(expectedService) &&
                            body.contains("RUNNING")

                println("Response code: ${response?.code}")
                println("Response body: $body")
                println("GET $path TEST: ${if (passed) "PASS" else "FAIL"}")

                if (passed) getPassed++
            } catch (e: Exception) {
                println("GET $path ERROR: ${e.message}")
            } finally {
                client.shutdown()
            }
        }

        // STEP 2: POST valid JSON to all services
        val postTests = listOf(
            Triple(
                "/traffic",
                "traffic-01",
                """{"event":"traffic-update","vehicleCount":42}"""
            ),
            Triple(
                "/environment",
                "environment-01",
                """{"event":"environment-update","temperature":24.5,"humidity":60}"""
            ),
            Triple(
                "/lighting",
                "lighting-01",
                """{"event":"lighting-status","state":"ON"}"""
            )
        )

        for ((path, serviceId, payload) in postTests) {
            println("\nTesting POST $path")
            val client = CoapClient("coap://127.0.0.1:5683$path")

            try {
                val response = client.post(
                    payload,
                    MediaTypeRegistry.APPLICATION_JSON
                )

                val body = response?.responseText ?: ""
                val passed =
                    response?.code == CoAP.ResponseCode.CHANGED &&
                            body.contains(serviceId)

                println("Response code: ${response?.code}")
                println("Response body: $body")
                println("POST $path TEST: ${if (passed) "PASS" else "FAIL"}")

                if (passed) postPassed++
            } catch (e: Exception) {
                println("POST $path ERROR: ${e.message}")
            } finally {
                client.shutdown()
            }
        }

        // STEP 3: Invalid JSON should return BAD_REQUEST
        println("\nTesting invalid JSON")

        val invalidClient = CoapClient(
            "coap://127.0.0.1:5683/traffic"
        )

        try {
            val response = invalidClient.post(
                """{"event":""",
                MediaTypeRegistry.APPLICATION_JSON
            )

            val passed =
                response?.code == CoAP.ResponseCode.BAD_REQUEST

            println("Invalid JSON response: ${response?.code}")
            println("INVALID JSON TEST: ${if (passed) "PASS" else "FAIL"}")

            if (passed) errorPassed++
        } catch (e: Exception) {
            println("INVALID JSON TEST ERROR: ${e.message}")
        } finally {
            invalidClient.shutdown()
        }

        // STEP 4: Empty body should return BAD_REQUEST
        println("\nTesting empty body")

        val emptyClient = CoapClient(
            "coap://127.0.0.1:5683/traffic"
        )

        try {
            val response = emptyClient.post(
                "",
                MediaTypeRegistry.APPLICATION_JSON
            )

            val passed =
                response?.code == CoAP.ResponseCode.BAD_REQUEST

            println("Empty body response: ${response?.code}")
            println("EMPTY BODY TEST: ${if (passed) "PASS" else "FAIL"}")

            if (passed) errorPassed++
        } catch (e: Exception) {
            println("EMPTY BODY TEST ERROR: ${e.message}")
        } finally {
            emptyClient.shutdown()
        }

        // STEP 5: Valid JSON array is not an accepted JSON object
        println("\nTesting JSON array")

        val arrayClient = CoapClient(
            "coap://127.0.0.1:5683/traffic"
        )

        try {
            val response = arrayClient.post(
                """[1,2,3]""",
                MediaTypeRegistry.APPLICATION_JSON
            )

            val passed =
                response?.code == CoAP.ResponseCode.BAD_REQUEST

            println("JSON array response: ${response?.code}")
            println("JSON ARRAY TEST: ${if (passed) "PASS" else "FAIL"}")

            if (passed) errorPassed++
        } catch (e: Exception) {
            println("JSON ARRAY TEST ERROR: ${e.message}")
        } finally {
            arrayClient.shutdown()
        }

        // STEP 6: Unknown endpoint should return NOT_FOUND.
        // This tests endpoint errors, not a missing registry entry.
        println("\nTesting unknown endpoint")

        val unknownClient = CoapClient(
            "coap://127.0.0.1:5683/unknown"
        )

        try {
            val response = unknownClient.get()

            val passed =
                response?.code == CoAP.ResponseCode.NOT_FOUND

            println("Unknown endpoint response: ${response?.code}")
            println("UNKNOWN ENDPOINT TEST: ${if (passed) "PASS" else "FAIL"}")

            if (passed) errorPassed++
        } catch (e: Exception) {
            println("UNKNOWN ENDPOINT TEST ERROR: ${e.message}")
        } finally {
            unknownClient.shutdown()
        }

        // STEP 7: Connection timeout/unavailable server test.
        // Port 5699 should have no CoAP server running.
        println("\nTesting unavailable CoAP server")

        val timeoutClient = CoapClient(
            "coap://127.0.0.1:5699/traffic"
        )
        timeoutClient.timeout = 2000L

        try {
            val response = timeoutClient.get()

            if (response == null) {
                println("UNAVAILABLE SERVER TEST: PASS (no response)")
                timeoutPassed++
            } else {
                println("Unexpected response: ${response.code}")
                println("UNAVAILABLE SERVER TEST: FAIL")
            }
        } catch (e: Exception) {
            println("Connection error received: ${e.message}")
            println("UNAVAILABLE SERVER TEST: PASS (connection failed)")
            timeoutPassed++
        } finally {
            timeoutClient.shutdown()
        }

        // FINAL RESULTS
        println("\n========== COAP TEST RESULTS ==========")
        println("GET tests: $getPassed/${getTests.size} passed")
        println("POST tests: $postPassed/${postTests.size} passed")
        println("Error tests: $errorPassed/4 passed")
        println("Unavailable server tests: $timeoutPassed/1 passed")

        val totalPassed =
            getPassed + postPassed + errorPassed + timeoutPassed

        val totalTests =
            getTests.size + postTests.size + 4 + 1

        println("TOTAL: $totalPassed/$totalTests tests passed")

    } catch (e: Exception) {
        println("CoAP TEST ERROR: ${e.message}")
        e.printStackTrace()
    } finally {
        server?.close()
        registry.stop()
        scope.cancel()
    }
}
