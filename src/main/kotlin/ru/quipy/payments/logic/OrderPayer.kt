package ru.quipy.payments.logic

import io.micrometer.core.instrument.MeterRegistry
import jakarta.annotation.PostConstruct
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.CallerBlockingRejectedExecutionHandler
import ru.quipy.common.utils.LeakyBucketMeter
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

@Service
class OrderPayer {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    @Autowired
    private lateinit var meterRegistry: MeterRegistry

    @Autowired
    private lateinit var paymentAccounts: List<PaymentExternalSystemAdapter>

    // Rate limiter на входе — leaky bucket (meter): каждый принятый заказ доливает 1, ведро вытекает со скоростью,
    // с которой мы гарантированно отправляем к провайдеру (для acc-23 ≈ 10.8 в секунду)
    private val bucket by lazy { LeakyBucketMeter(paymentAccounts.sumOf { it.processingSpeed() }) }

    // Вызов провайдера блокирующий: 110 запросов/с * 1 с ответа = 110 потоков
    private val paymentExecutor = ThreadPoolExecutor(
        110,
        110,
        0L,
        TimeUnit.MILLISECONDS,
        LinkedBlockingQueue(8_000),
        NamedThreadFactory("payment-submission-executor"),
        CallerBlockingRejectedExecutionHandler()
    )

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        val createdAt = System.currentTimeMillis()
        // Входящая нагрузка (Submit rate): заказ пришёл на оплату
        meterRegistry.counter("shop.payments.incoming").increment()
        paymentExecutor.submit {
            val createdEvent = paymentESService.create {
                it.create(
                    paymentId,
                    orderId,
                    amount
                )
            }
            logger.trace("Payment ${createdEvent.paymentId} for order $orderId created.")

            paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
        }
        return createdAt
    }

    // Gauge: сколько заказов сейчас внутри магазина
    // PostConstruct = вызови этот метод один раз, сразу после того как бин создан и в него внедрены все зависимости
    // потому что meterRegistry уже должен быть внедрен
    @PostConstruct
    fun registerGauges() {
        meterRegistry.gauge("shop.payments.inside", this) { it.inside() }
    }

    // Внутри магазина = принятые (incoming) − завершённые (completed) в любом статусе
    private fun inside() = meterRegistry.counter("shop.payments.incoming").count() -
        meterRegistry.find("shop.payments.completed").counters().sumOf { it.count() }

    // Back pressure: место в ведре есть - null, нет - момент (мс) для Retry-After.
    // Размер ведра - сколько заказов провайдер примет, пока заказ ещё может ждать (дедлайн − худший ответ),
    // но не меньше одной секунды провайдера: тест 1 (2.5 с) - 11, тест 2 (13 с) - 121, тест 3 (26 с) - 264
    fun retryAfterIfOverloaded(deadline: Long): Long? {
        val rate = paymentAccounts.sumOf { it.rateLimitPerSec() }
        // Ответ провайдера - от 0 до 2 средних (для acc-23 до 2 с)
        val worstResponse = paymentAccounts.maxOf { it.averageProcessingTime() }.multipliedBy(2)
        val canWaitSec = (deadline - now() - worstResponse.toMillis()) / 1000.0
        val waitMs = bucket.tryAdd(rate * maxOf(1.0, canWaitSec)) ?: return null

        meterRegistry.counter("shop.payments.rejected").increment()
        // Retry-After - момент, когда в ведре освободится место
        return now() + waitMs
    }
}