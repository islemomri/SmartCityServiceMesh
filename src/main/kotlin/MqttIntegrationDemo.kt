
package org.example

import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

fun main() {
    val topic = "smartcity/demo/${UUID.randomUUID()}"
    val received = CountDownLatch(1)

    val mqtt = MqttAdapter()

    try {
        mqtt.connect()

        mqtt.subscribe(topic) { receivedTopic, payload ->
            println("MQTT received on $receivedTopic: $payload")
            received.countDown()
        }

        // Give the broker a moment to register the subscription.
        Thread.sleep(500)

        mqtt.publish(
            topic = topic,
            payload = """{"service":"traffic-01","status":"RUNNING"}"""
        )

        if (received.await(5, TimeUnit.SECONDS)) {
            println("MQTT TEST: PASS")
        } else {
            println("MQTT TEST: FAIL - message not received in time")
        }
    } catch (e: Exception) {
        println("MQTT TEST ERROR: ${e.message}")
        e.printStackTrace()
    } finally {
        mqtt.close()
    }
}
