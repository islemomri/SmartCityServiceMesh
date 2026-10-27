
package org.example

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch

class MqttRouterBridge(
    private val mqtt: MqttAdapter,
    private val scope: CoroutineScope,
    private val router: MessageRouter,
    private val serviceId: String,
    private val topic: String,
    private val onRouted: (Boolean) -> Unit = {}
) {
    fun start() {
        mqtt.connect()

        mqtt.subscribe(topic) { receivedTopic, payload ->
            println("MQTT bridge received: $receivedTopic")

            scope.launch {
                try {
                    val accepted = router.route(
                        CityMessage(
                            destinationServiceId = serviceId,
                            content = payload
                        )
                    )

                    println("MQTT ROUTING RESULT: $accepted")
                    onRouted(accepted)
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    println("MQTT ROUTING ERROR: ${e.message}")
                    onRouted(false)
                }
            }
        }

        println("MQTT bridge started for $serviceId")
    }
}
