
package org.example

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken
import org.eclipse.paho.client.mqttv3.MqttCallback
import org.eclipse.paho.client.mqttv3.MqttClient
import org.eclipse.paho.client.mqttv3.MqttConnectOptions
import org.eclipse.paho.client.mqttv3.MqttMessage
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence
import java.nio.charset.StandardCharsets
import java.util.UUID

class MqttAdapter(
    brokerUrl: String = "tcp://test.mosquitto.org:1883",
    clientId: String = "smart-city-${UUID.randomUUID()}"
) : AutoCloseable {

    private val client = MqttClient(
        brokerUrl,
        clientId,
        MemoryPersistence()
    )

    fun connect() {
        if (client.isConnected) return

        val options = MqttConnectOptions().apply {
            isCleanSession = true
            isAutomaticReconnect = true
            connectionTimeout = 10
            keepAliveInterval = 30
        }

        client.connect(options)
        println("MQTT connected: ${client.serverURI}")
    }

    fun subscribe(
        topic: String,
        qos: Int = 1,
        handler: (String, String) -> Unit
    ) {
        require(topic.isNotBlank()) {
            "Topic cannot be blank"
        }
        require(qos in 0..2) {
            "QoS must be 0, 1, or 2"
        }
        check(client.isConnected) {
            "Connect to the MQTT broker before subscribing"
        }

        client.setCallback(object : MqttCallback {

            override fun connectionLost(cause: Throwable?) {
                println("MQTT connection lost: ${cause?.message}")
            }

            override fun messageArrived(
                topic: String?,
                message: MqttMessage?
            ) {
                if (topic != null && message != null) {
                    val payload = String(
                        message.payload,
                        StandardCharsets.UTF_8
                    )
                    handler(topic, payload)
                }
            }

            override fun deliveryComplete(
                token: IMqttDeliveryToken?
            ) {
                // No additional action needed for this demo.
            }
        })

        client.subscribe(topic, qos)
        println("MQTT subscribed: $topic")
    }

    fun publish(
        topic: String,
        payload: String,
        qos: Int = 1
    ) {
        require(topic.isNotBlank()) {
            "Topic cannot be blank"
        }
        require(qos in 0..2) {
            "QoS must be 0, 1, or 2"
        }
        check(client.isConnected) {
            "Connect to the MQTT broker before publishing"
        }

        val message = MqttMessage(
            payload.toByteArray(StandardCharsets.UTF_8)
        ).apply {
            this.qos = qos
        }

        client.publish(topic, message)
        println("MQTT published to $topic: $payload")
    }

    override fun close() {
        try {
            if (client.isConnected) {
                client.disconnect()
            }
        } finally {
            client.close()
            println("MQTT client closed")
        }
    }
}