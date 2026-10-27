
package org.example

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import org.eclipse.californium.core.CoapClient
import org.eclipse.californium.core.coap.CoAP

fun main() = runBlocking {
    val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    val registry = ServiceRegistry(scope)

    val services = listOf(
        CityService("traffic-01", "Traffic Service"),
        CityService("environment-01", "Environment Service"),
        CityService("lighting-01", "Smart Lighting Service")
    )

    val actors = services.associate { service ->
        service.id to ServiceActor(service.name, scope)
    }

    val mqttClients = mutableListOf<MqttAdapter>()
    var coapServer: CoapAdapter? = null
    var coapClient: CoapClient? = null

    try {
        services.forEach { registry.register(it) }
        actors.values.forEach { it.start() }

        val router = MessageRouter(registry, actors)

        suspend fun testMqttRoute(
            serviceId: String,
            topic: String,
            payload: String
        ): Boolean {
            val result = CompletableDeferred<Boolean>()

            val subscriber = MqttAdapter()
            mqttClients.add(subscriber)

            val bridge = MqttRouterBridge(
                mqtt = subscriber,
                scope = scope,
                router = router,
                serviceId = serviceId,
                topic = topic,
                onRouted = { accepted ->
                    result.complete(accepted)
                }
            )

            bridge.start()

            val publisher = MqttAdapter()
            mqttClients.add(publisher)
            publisher.connect()

            // Give the broker a short moment before publishing.
            delay(500)
            publisher.publish(topic, payload)

            return withTimeoutOrNull(10_000) {
                result.await()
            } ?: false
        }

        val trafficPassed = testMqttRoute(
            serviceId = "traffic-01",
            topic = SmartCityTopics.TRAFFIC_DATA,
            payload = """{"event":"traffic-update","vehicleCount":42}"""
        )
        println(
            "TRAFFIC MQTT TEST: ${if (trafficPassed) "PASS" else "FAIL"}"
        )

        val environmentPassed = testMqttRoute(
            serviceId = "environment-01",
            topic = SmartCityTopics.ENVIRONMENT_DATA,
            payload = """{"event":"environment-update","temperature":24.5}"""
        )
        println(
            "ENVIRONMENT MQTT TEST: ${if (environmentPassed) "PASS" else "FAIL"}"
        )

        val lightingPassed = testMqttRoute(
            serviceId = "lighting-01",
            topic = SmartCityTopics.LIGHTING_STATUS,
            payload = """{"event":"lighting-status","state":"ON"}"""
        )
        println(
            "LIGHTING MQTT TEST: ${if (lightingPassed) "PASS" else "FAIL"}"
        )

        coapServer = CoapAdapter(
            registry = registry,
            router = router,
            serviceId = "traffic-01",
            port = 5683
        )
        coapServer.start()

        coapClient = CoapClient("coap://127.0.0.1:5683/status")
        val response = coapClient.get()

        val coapPassed =
            response != null &&
                    response.code == CoAP.ResponseCode.CONTENT &&
                    response.responseText?.contains("traffic-01") == true &&
                    response.responseText.contains("RUNNING")

        println(
            "CoAP-REGISTRY TEST: ${if (coapPassed) "PASS" else "FAIL"}"
        )
        println("CoAP response: ${response?.responseText}")

        val allPassed =
            trafficPassed &&
                    environmentPassed &&
                    lightingPassed &&
                    coapPassed

        println(
            "PROTOCOL INTEGRATION: ${if (allPassed) "PASS" else "FAIL"}"
        )
    } catch (e: Exception) {
        println("PROTOCOL INTEGRATION ERROR: ${e.message}")
        e.printStackTrace()
    } finally {
        coapClient?.shutdown()
        coapServer?.close()

        mqttClients.asReversed().forEach { client ->
            try {
                client.close()
            } catch (e: Exception) {
                println("MQTT cleanup warning: ${e.message}")
            }
        }

        actors.values.forEach { actor ->
            try {
                actor.stop()
            } catch (e: Exception) {
                println("Actor cleanup warning: ${e.message}")
            }
        }

        registry.stop()
        scope.cancel()
    }
}
