package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import kotlinx.coroutines.delay
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.LeakingBucketRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) : PaymentExternalSystemAdapter {

    companion object {
        private val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        private val emptyBody = RequestBody.create(null, ByteArray(0))
        private val mapper = ObjectMapper().registerKotlinModule()

        // Tạo một OkHttpClient dùng chung
        private val client = OkHttpClient.Builder()
            .connectTimeout(5, TimeUnit.SECONDS)
            .readTimeout(10, TimeUnit.SECONDS)
            .writeTimeout(10, TimeUnit.SECONDS)
            .build()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val rateLimiter = LeakingBucketRateLimiter(
        rateLimitPerSec.toLong(),
        Duration.ofSeconds(1),
        parallelRequests
    )

    private val semaphore = Semaphore(parallelRequests)

    private val failureCount = AtomicInteger(0)
    private val maxFailures = 10
    private var isCircuitOpen = false
    private var circuitResetTime = System.currentTimeMillis()

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        if (isCircuitOpen) {
            if (System.currentTimeMillis() > circuitResetTime) {
                isCircuitOpen = false
                failureCount.set(0)
                logger.warn("[$accountName] Circuit breaker reset, resuming payments")
            } else {
                logger.warn("[$accountName] Circuit breaker open, rejecting request $paymentId")
                return
            }
        }

        while (!rateLimiter.tick()) {
            kotlinx.coroutines.runBlocking { delay(50) }
        }

        val remainingTime = deadline - now()
        if (remainingTime <= 0) {
            logger.warn("[$accountName] Payment expired, rejecting payment $paymentId")
            return
        }

        if (!semaphore.tryAcquire(remainingTime, TimeUnit.MILLISECONDS)) {
            logger.warn("[$accountName] Queueing request $paymentId due to high load")
            kotlinx.coroutines.runBlocking { delay(100) }

            if (!semaphore.tryAcquire(remainingTime - 100, TimeUnit.MILLISECONDS)) {
                logger.warn("[$accountName] Still overloaded, rejecting payment $paymentId")
                return
            }
        }

        try {
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

            val body = performHttpRequestWithRetry(request)

            if (body.result) {
                failureCount.set(0)
            } else {
                trackFailure()
            }

            logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

            paymentESService.update(paymentId) {
                it.logProcessing(body.result, now(), transactionId, reason = body.message)
            }
        } finally {
            semaphore.release()
        }
    }

    private fun performHttpRequestWithRetry(request: Request, maxRetries: Int = 5): ExternalSysResponse {
        var attempt = 0
        var waitTime = 500L

        while (attempt < maxRetries) {
            try {
                client.newCall(request).execute().use { response ->
                    return mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                }
            } catch (e: SocketTimeoutException) {
                logger.warn("[$accountName] Timeout on attempt ${attempt + 1}, retrying in $waitTime ms")
            } catch (e: Exception) {
                logger.error("[$accountName] Unexpected error on attempt ${attempt + 1}", e)
            }

            val jitter = (Math.random() * 100).toLong()
            Thread.sleep(waitTime + jitter)

            waitTime *= 2
            attempt++
        }

        return ExternalSysResponse("", "", false, "Max retries reached")
    }

    private fun trackFailure() {
        val count = failureCount.incrementAndGet()
        if (count >= maxFailures) {
            isCircuitOpen = true
            circuitResetTime = System.currentTimeMillis() + 30000 // 30s cooldown
            logger.warn("[$accountName] Circuit breaker triggered due to $count consecutive failures")
        }
    }

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName
}

public fun now() = System.currentTimeMillis()
