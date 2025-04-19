package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.newCoroutineContext
import okhttp3.*
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.common.utils.TokenBucketRateLimiter
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.*
import kotlin.math.pow
import kotlin.time.DurationUnit
import kotlin.time.toDuration
import kotlin.time.toDurationUnit
import kotlin.time.toJavaDuration


// Advice: always treat time as a Duration
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



    @OptIn(ExperimentalCoroutinesApi::class)
    private val dispatcher = Dispatchers.IO.limitedParallelism(1100 * 10)
    private val scope = CoroutineScope(dispatcher + SupervisorJob() + CoroutineExceptionHandler {_, ex ->
        logger.warn("WRONG EXEPTION: " + ex.message)
    })

    private val tokenBucket = TokenBucketRateLimiter(
        rate = rateLimitPerSec,
        bucketMaxCapacity = rateLimitPerSec,
        window = 1,
        timeUnit = TimeUnit.SECONDS,
    )

    private val client = OkHttpClient.Builder()
        .protocols(listOf(Protocol.HTTP_2, Protocol.HTTP_1_1))
        .dispatcher(
            Dispatcher().apply {
                maxRequests = 1100 * 10
                maxRequestsPerHost = 1100 * 10
            })
        .connectionPool(
            ConnectionPool(
                maxIdleConnections = 1100 * 1,
                keepAliveDuration = 10,
                timeUnit = TimeUnit.MINUTES,
            )
        )
        .connectTimeout(10, TimeUnit.SECONDS)
        .readTimeout(60, TimeUnit.SECONDS)
        .writeTimeout(60, TimeUnit.SECONDS)
        .callTimeout(
            properties.averageProcessingTime.toMillis().times(2).toDuration(DurationUnit.MILLISECONDS)
                .toJavaDuration()
        )
        .build()



    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {

        scope.launch {
            logger.warn("[$accountName] Submitting payment request for payment $paymentId")

            val transactionId = UUID.randomUUID()
            logger.info("[$accountName] Submit for $paymentId, txId: $transactionId")

            if (isDeadlineExceeded(deadline)) {
                logger.error("[$accountName] Payment deadline reached for txId: $transactionId, payment: $paymentId")

                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "deadline reached")
                }

                return@launch
            }

            // semaphore.acquire()

            try {
                // Логируем отправку платежа
                paymentESService.update(paymentId) {
                    it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
                }

                val request = Request.Builder()
                    .url("http://localhost:1234/external/process?serviceName=$serviceName&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                    .post(emptyBody)
                    .build()

                var responseBody: ExternalSysResponse? = null
                var currentTry = 0
                var success = false


                while (currentTry < retryCount && !success) {
                    if (currentTry > 0) {
                        while (!tokenBucket.tick()) {
                            delay(10)
                        }
                    }

                    currentTry += 1

                    if (isDeadlineExceeded(deadline)) {
                        logger.error("[$accountName] Payment deadline reached for txId: $transactionId, payment: $paymentId")

                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = "deadline reached")
                        }

                        return@launch
                    }

                    client.newCall(request).execute().use { response ->
                        val responseStr = response.body?.string()

                        responseBody = try {
                            mapper.readValue(responseStr, ExternalSysResponse::class.java)
                        } catch (e: Exception) {
                            logger.error("[$accountName] Ошибка при разборе ответа, код: ${response.code}, тело: $responseStr")
                            ExternalSysResponse(
                                transactionId.toString(),
                                paymentId.toString(),
                                false,
                                "Ошибка парсинга JSON"
                            )
                        }

                        success = responseBody!!.result

                    }
                }

                logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${responseBody?.result}, message: ${responseBody?.message}")

                paymentESService.update(paymentId) {
                    it.logProcessing(responseBody?.result ?: false, now(), transactionId, reason = responseBody?.message)
                }
            } catch (e: Exception) {
                when (e) {
                    is SocketTimeoutException -> {
                        logger.warn("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                        }
                    }

                    else -> {
                        logger.warn("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)

                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = e.message)
                        }
                    }
                }
            } finally {
                // semaphore.release()
            }
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
