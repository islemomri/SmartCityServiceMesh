
package org.example

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch

enum class ServiceStatus {
    RUNNING,
    UNREACHABLE
}

data class CityService(
    val id: String,
    val name: String,
    val status: ServiceStatus = ServiceStatus.RUNNING
)

private sealed interface RegistryMessage {

    data class Register(
        val service: CityService,
        val reply: CompletableDeferred<Unit>
    ) : RegistryMessage

    data class GetService(
        val id: String,
        val reply: CompletableDeferred<CityService?>
    ) : RegistryMessage

    data class GetAll(
        val reply: CompletableDeferred<List<CityService>>
    ) : RegistryMessage

    data class UpdateStatus(
        val id: String,
        val status: ServiceStatus,
        val reply: CompletableDeferred<Boolean>
    ) : RegistryMessage

    data class PrintServices(
        val reply: CompletableDeferred<Unit>
    ) : RegistryMessage
}

class ServiceRegistry(scope: CoroutineScope) {

    private val mailbox = Channel<RegistryMessage>(Channel.UNLIMITED)

    private val registryJob: Job = scope.launch {
        val services = mutableMapOf<String, CityService>()

        for (message in mailbox) {
            when (message) {

                is RegistryMessage.Register -> {
                    services[message.service.id] = message.service

                    println("Registered: ${message.service.name}")
                    message.reply.complete(Unit)
                }

                is RegistryMessage.GetService -> {
                    message.reply.complete(services[message.id])
                }

                is RegistryMessage.GetAll -> {
                    message.reply.complete(services.values.toList())
                }

                is RegistryMessage.UpdateStatus -> {
                    val service = services[message.id]

                    if (service == null) {
                        message.reply.complete(false)
                    } else {
                        services[message.id] = service.copy(
                            status = message.status
                        )
                        message.reply.complete(true)
                    }
                }

                is RegistryMessage.PrintServices -> {
                    println("\n=== Service Registry ===")

                    services.values.forEach { service ->
                        println("${service.name} - ${service.status}")
                    }

                    message.reply.complete(Unit)
                }
            }
        }
    }

    suspend fun register(service: CityService) {
        val reply = CompletableDeferred<Unit>()
        mailbox.send(RegistryMessage.Register(service, reply))
        reply.await()
    }

    suspend fun getService(id: String): CityService? {
        val reply = CompletableDeferred<CityService?>()
        mailbox.send(RegistryMessage.GetService(id, reply))
        return reply.await()
    }

    suspend fun getAllServices(): List<CityService> {
        val reply = CompletableDeferred<List<CityService>>()
        mailbox.send(RegistryMessage.GetAll(reply))
        return reply.await()
    }

    suspend fun updateStatus(
        id: String,
        status: ServiceStatus
    ): Boolean {
        val reply = CompletableDeferred<Boolean>()

        mailbox.send(
            RegistryMessage.UpdateStatus(id, status, reply)
        )

        return reply.await()
    }

    suspend fun printServices() {
        val reply = CompletableDeferred<Unit>()
        mailbox.send(RegistryMessage.PrintServices(reply))
        reply.await()
    }

    suspend fun stop() {
        mailbox.close()
        registryJob.join()
        println("Service Registry stopped")
    }
}
