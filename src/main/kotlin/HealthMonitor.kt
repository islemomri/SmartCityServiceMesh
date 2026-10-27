
package org.example

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeoutOrNull

private sealed interface MonitorMessage {
    data class Heartbeat(
        val serviceId: String,
        val reply: CompletableDeferred<Unit>
    ) : MonitorMessage

    data class Stop(
        val reply: CompletableDeferred<Unit>
    ) : MonitorMessage
}

class HealthMonitor(
    private val registry: ServiceRegistry,
    private val checkIntervalMillis: Long = 1000L,
    private val timeoutMillis: Long = 3000L
) {
    private val mailbox = Channel<MonitorMessage>(Channel.UNLIMITED)
    private var monitoringJob: Job? = null

    fun start(scope: CoroutineScope) {
        if (monitoringJob?.isActive == true) return

        require(checkIntervalMillis > 0) {
            "Check interval must be greater than zero"
        }
        require(timeoutMillis > 0) {
            "Timeout must be greater than zero"
        }

        monitoringJob = scope.launch {
            // This state belongs exclusively to the monitor actor.
            val lastHeartbeat = mutableMapOf<String, Long>()

            println("Automatic Health Monitor started")

            var running = true
            var nextHealthCheck =
                System.currentTimeMillis() + checkIntervalMillis

            while (running && isActive) {
                val now = System.currentTimeMillis()

                // Run health checks when their deadline is reached.
                if (now >= nextHealthCheck) {
                    checkHealth(lastHeartbeat)
                    nextHealthCheck =
                        System.currentTimeMillis() + checkIntervalMillis
                    continue
                }

                val remaining = nextHealthCheck - now

                val message = withTimeoutOrNull(remaining) {
                    mailbox.receive()
                }

                if (message == null) {
                    continue
                }

                when (message) {
                    is MonitorMessage.Heartbeat -> {
                        try {
                            val service =
                                registry.getService(message.serviceId)

                            if (service == null) {
                                println(
                                    "Unknown service: ${message.serviceId}"
                                )
                            } else {
                                lastHeartbeat[message.serviceId] =
                                    System.currentTimeMillis()

                                registry.updateStatus(
                                    message.serviceId,
                                    ServiceStatus.RUNNING
                                )

                                //println(
                                   // "Heartbeat received: ${service.name}"
                               // )
                            }

                            message.reply.complete(Unit)
                        } catch (e: CancellationException) {
                            message.reply.cancel(e)
                            throw e
                        } catch (e: Exception) {
                            message.reply.completeExceptionally(e)
                        }
                    }

                    is MonitorMessage.Stop -> {
                        message.reply.complete(Unit)
                        running = false
                    }
                }
            }
        }
    }


    suspend fun receiveHeartbeat(serviceId: String) {
        val reply = CompletableDeferred<Unit>()

        mailbox.send(
            MonitorMessage.Heartbeat(
                serviceId = serviceId,
                reply = reply
            )
        )

        reply.await()
    }

    private suspend fun checkHealth(
        lastHeartbeat: Map<String, Long>
    ) {
        val now = System.currentTimeMillis()

        for (service in registry.getAllServices()) {
            val lastTime = lastHeartbeat[service.id]

            val isUnreachable =
                lastTime == null || now - lastTime > timeoutMillis

            if (isUnreachable) {
                if (service.status != ServiceStatus.UNREACHABLE) {
                    println(
                        "ALERT: ${service.name} is UNREACHABLE"
                    )
                }

                registry.updateStatus(
                    service.id,
                    ServiceStatus.UNREACHABLE
                )
            } else if (service.status != ServiceStatus.RUNNING) {
                registry.updateStatus(
                    service.id,
                    ServiceStatus.RUNNING
                )
            }
        }
    }

    suspend fun stop() {
        val job = monitoringJob ?: return

        if (job.isActive) {
            val reply = CompletableDeferred<Unit>()

            mailbox.send(MonitorMessage.Stop(reply))
            reply.await()
        }

        job.join()
        monitoringJob = null

        println("Health Monitor stopped")
    }
}
