package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.Semaphore

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

    private val client = OkHttpClient.Builder()
        .callTimeout(Duration.ofMillis(properties.averageProcessingTime.toMillis()*2))

        .retryOnConnectionFailure(true)
        .build()

    private val pool = Executors.newFixedThreadPool(parallelRequests)

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

        pool.submit {
            try {
                rateLimiter.tickBlocking()

                if (isDeadlineExceeded(deadline)) {
                    logTimeout(paymentId, transactionId, "Request expired before execution.")
                    return@submit
                }

                val waitStart = now()
                semaphore.acquire()
                val waitTime = now() - waitStart
                logger.warn("[$accountName] Semaphore wait time: $waitTime ms")

                if (isDeadlineExceeded(deadline)) {
                    logTimeout(paymentId, transactionId, "Request expired after acquiring semaphore.")
                    return@submit
                }

                var currentTry = 0
                var success = false

                while (currentTry <= retryCount) {
                    currentTry += 1
                    if (success) {
                        break
                    }

                    if (currentTry > 1) {
                        rateLimiter.tickBlocking()
                    }

                    try {
                        client.newCall(request).execute().use { response ->
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

                            success = body.result
                        }
                    } catch (e: SocketTimeoutException) {
                        logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                        logTimeout(paymentId, transactionId, "Request timeout.")
                    } catch (e: Exception) {
                        logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = e.message)
                        }
                    }
                }
            } finally {
                semaphore.release()
            }
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