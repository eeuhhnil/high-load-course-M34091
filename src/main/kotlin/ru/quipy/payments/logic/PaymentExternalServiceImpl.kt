package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.OkHttpClient
import okhttp3.Protocol
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.OnlineShopApplication
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.ExecutorService
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit

// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) : PaymentExternalSystemAdapter {
    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
        val semaphore = Semaphore(20000, true)
        private val executorService: ExecutorService = OnlineShopApplication.appExecutor
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests
    private val rateLimiter = SlidingWindowRateLimiter(rate = rateLimitPerSec.toLong(), window = Duration.ofSeconds(1))

    private val client = OkHttpClient.Builder()
        .callTimeout(13000L, TimeUnit.MILLISECONDS)
        .protocols(listOf(Protocol.HTTP_2, Protocol.HTTP_1_1))
        .build()

//    private val newClient = HTTP2Client()

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")
        val transactionId = UUID.randomUUID()
        logger.info("[$accountName] Submit for $paymentId , txId: $transactionId")
        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        val request = Request.Builder().run {
            url("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
            post(emptyBody)
        }.build()

        val maxRetries = 4
        var attempt = 1
        var retryDelay = 200L
        var accumDelay = 0L
        executorService.submit {
            while (attempt <= maxRetries) {
                semaphore.acquire()
                try {
                    rateLimiter.tickBlocking()
                    // client.newCall(request).execute().use
                    client.newCall(request).execute().use { response ->
                        if (response.isSuccessful) {
                            val body = try {
                                mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                            } catch (e: Exception) {
                                logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                                ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                            }
                            logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")
                            // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                            // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                            paymentESService.update(paymentId) {
                                it.logProcessing(body.result, now(), transactionId, reason = body.message)
                            }
                            if (body.result) return@submit
                        } else if (response.code in 500..599 || response.code == 429) {
                            logger.warn("[$accountName] Retrying payment due to server error (${response.code}) for txId: $transactionId, payment: $paymentId")
                        } else {
                            logger.error("[$accountName] Payment failed with non-retriable error (${response.code}) for txId: $transactionId, payment: $paymentId")
                        }
                    }
                } catch (e: Exception) {
                    when (e) {
                        is SocketTimeoutException -> {
                            logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                            paymentESService.update(paymentId) {
                                it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                            }
                        }
                        else -> {
                            logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
                            paymentESService.update(paymentId) {
                                it.logProcessing(false, now(), transactionId, reason = e.message)
                            }
                        }
                    }
                } finally {
                    semaphore.release()
                }
                attempt++
                accumDelay += retryDelay
                if (attempt > maxRetries || accumDelay + requestAverageProcessingTime.toMillis() + 100L >= 50000L) break
                Thread.sleep(retryDelay)
                retryDelay *= 2
            }

            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Retries exhausted.")
            }
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()