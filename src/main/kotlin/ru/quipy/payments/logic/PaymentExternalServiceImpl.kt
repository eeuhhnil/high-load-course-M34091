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

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) : PaymentExternalSystemAdapter {

    companion object {
        private val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        private val emptyBody = RequestBody.create(null, ByteArray(0))
        private val mapper = ObjectMapper().registerKotlinModule()

        // Dùng `companion object` để tái sử dụng `OkHttpClient`
        private val client = OkHttpClient.Builder()
            .connectTimeout(5, TimeUnit.SECONDS)  // Thời gian kết nối tối đa
            .readTimeout(10, TimeUnit.SECONDS)    // Thời gian đọc dữ liệu tối đa
            .writeTimeout(10, TimeUnit.SECONDS)   // Thời gian ghi dữ liệu tối đa
            .build()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    // Khởi tạo Leaking Bucket Rate Limiter
    private val rateLimiter = LeakingBucketRateLimiter(
        rateLimitPerSec.toLong(),  // Giới hạn request mỗi giây
        Duration.ofSeconds(1),     // Thời gian tính toán giới hạn
        parallelRequests           // Dung lượng xô (số request tối đa có thể chứa)
    )

    // Semaphore để giới hạn số request đồng thời
    private val semaphore = Semaphore(parallelRequests)

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        // Điều chỉnh kiểm soát tốc độ request với delay thay vì `Thread.sleep(100)`
        while (!rateLimiter.tick()) {
            kotlinx.coroutines.runBlocking { delay(50) } // Giảm tải CPU, tránh busy-waiting
        }

        // Tính thời gian còn lại trước deadline để tối ưu `semaphore.tryAcquire()`
        val remainingTime = deadline - now()
        if (remainingTime <= 0 || !semaphore.tryAcquire(remainingTime, TimeUnit.MILLISECONDS)) {
            logger.warn("[$accountName] Too many concurrent requests, rejecting payment $paymentId")
            return
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

            logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

            paymentESService.update(paymentId) {
                it.logProcessing(body.result, now(), transactionId, reason = body.message)
            }
        } finally {
            semaphore.release() // Giải phóng slot trong semaphore sau khi xử lý xong
        }
    }

    // Hàm retry request với Exponential Backoff
    private fun performHttpRequestWithRetry(request: Request, maxRetries: Int = 3): ExternalSysResponse {
        var attempt = 0
        var waitTime = 500L // Bắt đầu với 500ms

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

            Thread.sleep(waitTime)
            waitTime *= 2 // Exponential backoff
            attempt++
        }

        return ExternalSysResponse("", "", false, "Max retries reached")
    }

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName
}

// Hàm lấy thời gian hiện tại
public fun now() = System.currentTimeMillis()
