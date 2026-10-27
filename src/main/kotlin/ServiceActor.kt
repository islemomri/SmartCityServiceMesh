package org.example

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch

private data class ActorMessage(
    val content: String,
    val completed: CompletableDeferred<Unit>
)

class ServiceActor(
    private val serviceName: String,
    private val scope: CoroutineScope
) {
    private val mailbox = Channel<ActorMessage>(capacity = 10)
    private var actorJob: Job? = null

    fun start() {
        if (actorJob?.isActive == true) return

        actorJob = scope.launch {
            for (message in mailbox) {
                try {
                    //println(
                       // "[$serviceName] Message received: ${message.content}"
                  //  )

                    processMessage(message.content)
                    message.completed.complete(Unit)
                } catch (e: CancellationException) {
                    message.completed.cancel(e)
                    throw e
                } catch (e: Exception) {
                    println(
                        "[$serviceName] Message failed: ${e.message}"
                    )

                    message.completed.completeExceptionally(e)

                    // Keep the actor alive for the next message.
                }
            }
        }
    }

    suspend fun send(message: String) {
        check(actorJob?.isActive == true) {
            "Actor $serviceName is not running"
        }

        val acknowledgement = CompletableDeferred<Unit>()

        mailbox.send(
            ActorMessage(
                content = message,
                completed = acknowledgement
            )
        )

        acknowledgement.await()
    }

    private fun processMessage(message: String) {
       // println("[$serviceName] Processing: $message")
    }

    suspend fun stop() {
        mailbox.close()
        actorJob?.join()
        actorJob = null

        println("[$serviceName] Actor stopped")
    }
}