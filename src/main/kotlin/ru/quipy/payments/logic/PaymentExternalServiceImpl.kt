package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import okhttp3.Call
import okhttp3.EventListener
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.IOException
import java.io.InterruptedIOException
import java.time.Duration
import java.util.*
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val meterRegistry: MeterRegistry,
    private val attemptTimeoutMillis: Long? = null,
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

    // Не более rateLimitPerSec вызовов провайдера в любом отрезке длиной 1 с + 10 мс запаса, лишние ждут разрешения.
    // Провайдер считает свою секунду по своим часам и получает запрос с сетевой задержкой:
    // без запаса 12-й вызов иногда попадал в его секунду (rate_limit_breached)
    private val rateWindow = Duration.ofMillis(1010)
    private val rateLimiter = SlidingWindowRateLimiter(
        rate = rateLimitPerSec.toLong(),
        window = rateWindow,
    )

    // Ждущие разрешения получают его по порядку прихода (честная блокировка). Без неё tickBlocking()
    // отдаёт освободившееся место случайному из ждущих, и на пиках «невезучие» заказы ждали дольше дедлайна
    private val rateLimiterQueue = ReentrantLock(true)

    // Длительность HTTP-попытки вместе с чтением тела. Таймауты видны отдельной серией.
    private val providerTimers = listOf("response", "timeout", "io_error").associateWith { outcome ->
        Timer.builder("shop.provider.duration")
            .tags("account", accountName, "outcome", outcome)
            .serviceLevelObjectives(
                Duration.ofMillis(50), Duration.ofMillis(100), Duration.ofMillis(200),
                Duration.ofMillis(300), Duration.ofMillis(500), Duration.ofMillis(750),
                Duration.ofSeconds(1), Duration.ofMillis(1250), Duration.ofMillis(1500),
                Duration.ofSeconds(2), Duration.ofMillis(2500), Duration.ofSeconds(3),
                Duration.ofSeconds(4), Duration.ofSeconds(5), Duration.ofSeconds(6)
            )
            .register(meterRegistry)
    }

    // Не более parallelRequests вызовов провайдера одновременно, иначе провайдер отвечает
    // parallel_requests_limit_breached; fair = true слоты выдаются по очереди, ранние заказы не обгоняются поздними
    private val parallelLimiter = Semaphore(parallelRequests, true)

    // Исходящая нагрузка: сколько вызовов провайдера уходит (за каждый платим price)
    private val outgoingCounter = meterRegistry.counter("shop.payments.outgoing", "account", accountName)

    private val client = OkHttpClient.Builder()
        .eventListenerFactory {
            object : EventListener() {
                private val sample = Timer.start(meterRegistry)

                override fun callEnd(call: Call) {
                    sample.stop(providerTimers.getValue("response"))
                }

                override fun callFailed(call: Call, ioe: IOException) {
                    val outcome = if (ioe is InterruptedIOException) "timeout" else "io_error"
                    sample.stop(providerTimers.getValue(outcome))
                }
            }
        }
        .build()

    private val retryBudgetMillis = attemptTimeoutMillis ?: requestAverageProcessingTime.multipliedBy(2).toMillis()

    // Повторы всегда ограничиваем: каждая попытка стоит price. Не больше 3 попыток на оплату
    private val maxAttempts = 3

    // Исходы, после которых повтор может пройти: провайдер отказал (result = false, в том числе 429 и 5xx),
    // не дождались ответа или сетевая ошибка. Ошибка в самом запросе (прочие 4xx) от повтора не исправится
    private val retriable = setOf("fail", "timeout", "error")

    // Повторные вызовы провайдера (вторая и третья попытки)
    private val retryCounter = meterRegistry.counter("shop.payments.retries", "account", accountName)

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        // На повтор оставляем время минимум на одну попытку.
        var attempts = 1
        var outcome = pay(paymentId, amount, paymentStartedAt, deadline)
        while (outcome in retriable && attempts < maxAttempts && deadline - now() >= retryBudgetMillis) {
            attempts++
            outcome = pay(paymentId, amount, paymentStartedAt, deadline, retry = true)
        }
        completed(outcome)
    }

    // Одна попытка оплаты: свой transactionId, свои logSubmission / logProcessing. Возвращает исход попытки
    private fun pay(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long, retry: Boolean = false): String {
        val transactionId = UUID.randomUUID()

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        // Сначала слот окна (ждать можно долго), потом rate limiter (разрешение — прямо перед отправкой)
        // acquire() до try: если он не выполнился, finally не должен освобождать чужой слот
        parallelLimiter.acquire()
        try {
            // Разрешение берём непосредственно перед отправкой, чтобы между ним и приходом запроса к провайдеру не было лишней работы, которая могла бы сдвинуть запросы в одно окно провайдера
            rateLimiterQueue.withLock { rateLimiter.tickBlocking() }

            // После лимитеров бюджет мог сократиться.
            val remaining = deadline - now()
            if (remaining <= 0 || (retry && attemptTimeoutMillis != null && remaining < attemptTimeoutMillis)) {
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Not enough time before the deadline.")
                }
                return "timeout"
            }

            val request = Request.Builder().run {
                url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                post(emptyBody)
            }.build()

            val callTimeoutMillis = minOf(remaining, attemptTimeoutMillis ?: remaining)
            val call = client.newCall(request).apply { timeout().timeout(callTimeoutMillis, TimeUnit.MILLISECONDS) }
            outgoingCounter.increment()
            if (retry) retryCounter.increment()
            call.execute().use { response ->
                val body = try {
                    mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                    ExternalSysResponse(transactionId.toString(), paymentId.toString(),false, e.message)
                }

                logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                paymentESService.update(paymentId) {
                    it.logProcessing(body.result, now(), transactionId, reason = body.message)
                }
                return when {
                    body.result -> "success"
                    response.code in 400..499 && response.code != 429 -> "rejected"
                    else -> "fail"
                }
            }
        } catch (e: Exception) {
            return when (e) {
                // Не дождались ответа: истекло время до дедлайна (таймаут вызова) или таймаут сокета
                is InterruptedIOException -> {
                    logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                    }
                    "timeout"
                }

                else -> {
                    logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)

                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = e.message)
                    }
                    "error"
                }
            }
        } finally {
            parallelLimiter.release()
        }
    }

    // Завершённые оплаты по итоговому исходу (один раз на оплату, после всех попыток):
    // rate(completed) — Processing speed, incoming − completed — заказы внутри магазина
    private fun completed(outcome: String) =
        meterRegistry.counter("shop.payments.completed", "account", accountName, "outcome", outcome).increment()

    // Скорость, которую выдерживаем гарантированно: rateLimitPerSec вызовов за окно 1.01 с плюс до 10 мс,
    // пока tickBlocking() заметит свободное место (проверяет раз в 10 мс). Для acc-23: 11 / 1.02 с ≈ 10.8 в секунду
    override fun processingSpeed() =
        minOf(
            rateLimitPerSec * 1000.0 / (rateWindow.toMillis() + 10),
            parallelRequests * 1000.0 / requestAverageProcessingTime.toMillis()
        )

    override fun averageProcessingTime(): Duration = requestAverageProcessingTime

    override fun rateLimitPerSec() = rateLimitPerSec

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()
