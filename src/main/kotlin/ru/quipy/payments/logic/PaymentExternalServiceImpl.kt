package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.RateLimiter
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import kotlinx.coroutines.*
import java.util.concurrent.LinkedBlockingQueue

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
    private var rateLimiter: RateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))
    private val parallelRequests = properties.parallelRequests

    private val client = OkHttpClient.Builder().build()

    private val requestQueue = LinkedBlockingQueue<() -> Unit>()

    init {
        repeat(parallelRequests) {
            GlobalScope.launch {
                while (true) {
                    val task = requestQueue.take()
                    task()
                }
            }
        }
    }

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        requestQueue.offer {
            processPaymentWithRetry(paymentId, amount, paymentStartedAt, deadline)
        }
    }

    private fun processPaymentWithRetry(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long, retries: Int = 3) {
        var attempt = 0
        val transactionId = UUID.randomUUID()

        while (attempt <= retries) {
            try {
                if (!rateLimiter.tick()) {
                    Thread.sleep(50)
                    continue
                }

                logger.info("[$accountName] Attempt ${attempt + 1}: Processing payment $paymentId, txId: $transactionId")

                paymentESService.update(paymentId) {
                    it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
                }

                val request = Request.Builder().run {
                    url("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                    post(emptyBody)
                }.build()

                client.newCall(request).execute().use { response ->
                    val body = try {
                        mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error("[$accountName] [ERROR] Failed to parse response for txId: $transactionId, payment: $paymentId")
                        ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                    }

                    if (body.result) {
                        logger.info("[$accountName] Payment SUCCESS for txId: $transactionId, payment: $paymentId")
                        paymentESService.update(paymentId) {
                            it.logProcessing(true, now(), transactionId, reason = body.message)
                        }
                        return // Thành công, không cần retry
                    } else {
                        logger.warn("[$accountName] Payment FAILED for txId: $transactionId, attempt ${attempt + 1}")
                        attempt++
                        Thread.sleep(100L * attempt)
                    }
                }
            } catch (e: SocketTimeoutException) {
                logger.error("[$accountName] Timeout on attempt ${attempt + 1} for txId: $transactionId")
                attempt++
                Thread.sleep(100L * attempt)
            } catch (e: Exception) {
                logger.error("[$accountName] General error on attempt ${attempt + 1}: ${e.message}")
                attempt++
                Thread.sleep(100L * attempt)
            }
        }

        logger.error("[$accountName] Payment FAILED after $retries retries for txId: $transactionId")
        paymentESService.update(paymentId) {
            it.logProcessing(false, now(), transactionId, reason = "Failed after retries")
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName
}

public fun now() = System.currentTimeMillis()
