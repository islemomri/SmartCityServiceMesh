
package org.example

import kotlinx.coroutines.CancellationException
import kotlin.system.measureTimeMillis

data class CityMessage(
    val destinationServiceId: String,
    val content: String
)

class MessageRouter(
    private val registry: ServiceRegistry,
    private val actors: Map<String, ServiceActor>,
    private val backupServices: Map<String, String> = emptyMap()
) {

    suspend fun route(message: CityMessage): Boolean {
        val primaryId = message.destinationServiceId

        //println("\n=== AUTOMATIC ROUTING ===")
        //println("Primary service: $primaryId")

        // Try the primary service first.
        if (attemptDelivery(primaryId, message.content, "PRIMARY")) {
            return true
        }

        // Find a compatible backup, if one is configured.
        val backupId = backupServices[primaryId]

        if (backupId == null) {
            println("REROUTING FAILED: No compatible backup for $primaryId")
            return false
        }

        println("PRIMARY FAILED: $primaryId")
        println("REROUTING TO BACKUP: $backupId")

        if (attemptDelivery(backupId, message.content, "BACKUP")) {
            println("REROUTING SUCCESS: Delivered through $backupId")
            return true
        }

        println("REROUTING FAILED: Backup $backupId is unavailable")
        return false
    }

    private suspend fun attemptDelivery(
        serviceId: String,
        content: String,
        role: String
    ): Boolean {
        val service = registry.getService(serviceId)

        if (service == null) {
            println("$role FAILED: Service not found: $serviceId")
            return false
        }

        if (service.status != ServiceStatus.RUNNING) {
            println("$role FAILED: ${service.name} is ${service.status}")
            return false
        }

        val actor = actors[serviceId]

        if (actor == null) {
            println("$role FAILED: No actor for ${service.name}")
            return false
        }

        return try {
            var elapsedMs = 0L

            measureTimeMillis {
                actor.send(content)
            }.also {
                elapsedMs = it
            }

            //println("$role SUCCESS: ${service.name}")
            //println("Delivery time: $elapsedMs ms")
            true
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            println("$role DELIVERY FAILED: ${service.name}: ${e.message}")
            false
        }
    }
}
