package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.*
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.IOException
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests
    private val retryCount = 3

    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))
    private val semaphore = Semaphore(parallelRequests)

    private val client =
        OkHttpClient.Builder()
            .dispatcher(
                Dispatcher().apply {
                    maxRequests = parallelRequests
                    maxRequestsPerHost = parallelRequests
                }
            )
            .connectionPool(
                ConnectionPool(
                    maxIdleConnections = parallelRequests+1000,
                    keepAliveDuration = 10,
                    timeUnit = TimeUnit.MINUTES
                )
            )
            .callTimeout(Duration.ofSeconds(requestAverageProcessingTime.seconds * 2))
            .build()


    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()
        logger.info("[$accountName] Submit for $paymentId , txId: $transactionId")

        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        val request = Request.Builder().run {
            url("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
            post(emptyBody)
        }.build()

        if (isDeadlineExceeded(deadline)) {
            logTimeout(paymentId, transactionId, "Request expired before execution.")
            return
        }

        try {
            rateLimiter.tickBlocking()

            if (isDeadlineExceeded(deadline)) {
                logTimeout(paymentId, transactionId, "Request expired before execution.")
                return
            }

            val waitStart = now()
            semaphore.acquire()
            val waitTime = now() - waitStart
            logger.warn("[$accountName] Semaphore wait time: $waitTime ms")

            if (isDeadlineExceeded(deadline)) {
                logTimeout(paymentId, transactionId, "Request expired after acquiring semaphore.")
                semaphore.release()
                return
            }

            var currentTry = 0

            fun trySend() {
                if (currentTry++ > retryCount || isDeadlineExceeded(deadline)) {
                    semaphore.release()
                    return
                }

                if (currentTry > 1) {
                    rateLimiter.tickBlocking()
                }

                client.newCall(request).enqueue(object : Callback {
                    override fun onResponse(call: Call, response: Response) {
                        response.use {
                            val body = try {
                                mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                            } catch (e: Exception) {
                                logger.error("[$accountName] [ERROR] Payment failed for txId: $transactionId, payment: $paymentId, code: ${response.code}, reason: ${response.body?.string()}")
                                ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                            }

                            logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                            paymentESService.update(paymentId) {
                                it.logProcessing(body.result, now(), transactionId, reason = body.message)
                            }

                            if (!body.result && !isDeadlineExceeded(deadline)) {
                                trySend()
                            } else {
                                semaphore.release()
                            }
                        }
                    }

                    override fun onFailure(call: Call, e: IOException) {
                        logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)

                        val reason = e.message ?: "Unknown error"
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = reason)
                        }

                        if (e is SocketTimeoutException && !isDeadlineExceeded(deadline)) {
                            logger.warn("[$accountName] Retrying after timeout for txId: $transactionId")
                            trySend()
                        } else {
                            semaphore.release()
                        }
                    }
                })
            }

            trySend()

        } catch (e: Exception) {
            logger.error("[$accountName] Unexpected error while preparing payment $paymentId", e)
            logTimeout(paymentId, transactionId, "Unexpected error: ${e.message}")
        }
    }



    private fun logTimeout(paymentId: UUID, transactionId: UUID, reason: String) {
        logger.error("[$accountName] Timeout: $reason for txId: $transactionId, payment: $paymentId")
        paymentESService.update(paymentId) {
            it.logProcessing(false, now(), transactionId, reason = reason)
        }
    }

    private fun isDeadlineExceeded(deadline: Long): Boolean {
        return now() + requestAverageProcessingTime.toMillis() > deadline
    }

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName
}

public fun now() = System.currentTimeMillis()