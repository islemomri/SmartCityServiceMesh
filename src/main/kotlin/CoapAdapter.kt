
package org.example

import com.google.gson.JsonParser
import kotlinx.coroutines.runBlocking
import org.eclipse.californium.core.CoapResource
import org.eclipse.californium.core.CoapServer
import org.eclipse.californium.core.config.CoapConfig
import org.eclipse.californium.core.coap.CoAP
import org.eclipse.californium.core.coap.MediaTypeRegistry
import org.eclipse.californium.core.server.resources.CoapExchange
import org.eclipse.californium.elements.config.Configuration

class CoapAdapter(
    private val registry: ServiceRegistry,
    private val router: MessageRouter,
    private val serviceId: String = "traffic-01",
    private val port: Int = 5683
) : AutoCloseable {

    private var server: CoapServer? = null

    fun start() {
        check(server == null) {
            "CoAP server is already started"
        }

        CoapConfig.register()

        val configuration = Configuration.createStandardWithoutFile()
        val coapServer = CoapServer(configuration, port)

        // Preserve the original /status endpoint
        coapServer.add(createStatusResource("status", serviceId))

        // Status endpoints for all three services
        coapServer.add(createStatusResource("traffic", "traffic-01"))
        coapServer.add(createStatusResource("environment", "environment-01"))
        coapServer.add(createStatusResource("lighting", "lighting-01"))

        coapServer.start()
        server = coapServer

        println("CoAP server started on port $port")
        println("CoAP resources: /status, /traffic, /environment, /lighting")
    }

    private fun createStatusResource(
        path: String,
        targetServiceId: String
    ): CoapResource {

        return object : CoapResource(path) {

            override fun handleGET(exchange: CoapExchange) {
                try {
                    val service = runBlocking {
                        registry.getService(targetServiceId)
                    }

                    if (service == null) {
                        exchange.respond(
                            CoAP.ResponseCode.NOT_FOUND,
                            "Service not found: $targetServiceId"
                        )
                        return
                    }

                    val body =
                        """{"service":"${service.id}","name":"${service.name}","status":"${service.status}","protocol":"CoAP"}"""

                    exchange.respond(
                        CoAP.ResponseCode.CONTENT,
                        body,
                        MediaTypeRegistry.APPLICATION_JSON
                    )

                    println(
                        "CoAP GET /$path: ${service.id} = ${service.status}"
                    )

                } catch (e: Exception) {
                    println("CoAP GET /$path error: ${e.message}")

                    exchange.respond(
                        CoAP.ResponseCode.INTERNAL_SERVER_ERROR,
                        "Unable to read service status"
                    )
                }
            }

            override fun handlePOST(exchange: CoapExchange) {
                val payload = exchange.requestText

                // 1. Reject an empty request
                if (payload.isNullOrBlank()) {
                    exchange.respond(
                        CoAP.ResponseCode.BAD_REQUEST,
                        "Request body cannot be empty"
                    )
                    return
                }

                // 2. Validate JSON syntax and require a JSON object
                val validJson = try {
                    JsonParser.parseString(payload).isJsonObject
                } catch (e: Exception) {
                    false
                }

                if (!validJson) {
                    exchange.respond(
                        CoAP.ResponseCode.BAD_REQUEST,
                        "Invalid JSON: expected a JSON object"
                    )
                    return
                }

                // 3. Route valid JSON to the destination service
                try {
                    val delivered = runBlocking {
                        router.route(
                            CityMessage(
                                destinationServiceId = targetServiceId,
                                content = payload
                            )
                        )
                    }

                    if (delivered) {
                        exchange.respond(
                            CoAP.ResponseCode.CHANGED,
                            """{"result":"success","serviceId":"$targetServiceId"}""",
                            MediaTypeRegistry.APPLICATION_JSON
                        )
                    } else {
                        exchange.respond(
                            CoAP.ResponseCode.SERVICE_UNAVAILABLE,
                            "Message could not be delivered to $targetServiceId"
                        )
                    }

                } catch (e: Exception) {
                    println("CoAP POST /$path error: ${e.message}")

                    exchange.respond(
                        CoAP.ResponseCode.INTERNAL_SERVER_ERROR,
                        "Unable to process message"
                    )
                }
            }
        }
    }

    override fun close() {
        val activeServer = server ?: return
        server = null

        activeServer.stop()
        activeServer.destroy()

        println("CoAP server stopped")
    }
}
