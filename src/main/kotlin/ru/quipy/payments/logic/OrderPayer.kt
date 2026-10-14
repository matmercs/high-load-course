package ru.quipy.payments.logic

import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.CallerBlockingRejectedExecutionHandler
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.time.Duration
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

    // Запас на разброс времени ответа провайдера
    private val deadlineMargin = Duration.ofSeconds(10)

    private val paymentExecutor = ThreadPoolExecutor(
        16,
        16,
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


     // Успеет ли новый заказ к дедлайну, если поставить его в очередь
     // Ожидание = заказов внутри магазина (incoming − completed, наша метрика) / Processing speed
     // + время обработки + запас. Успеет - null; не успеет - момент мс для Retry-After
    fun retryAfterIfOverloaded(deadline: Long): Long? {
        val speed = paymentAccounts.sumOf { it.processingSpeed() }
        val inside = meterRegistry.counter("shop.payments.incoming").count() -
            meterRegistry.find("shop.payments.completed").counters().sumOf { it.count() }
        val processing = paymentAccounts.maxOf { it.averageProcessingTime() }
        val expectedFinish = now() + (inside / speed * 1000).toLong() + processing.toMillis() + deadlineMargin.toMillis()
        if (expectedFinish <= deadline) return null

        meterRegistry.counter("shop.payments.rejected").increment()
        // Место в очереди освобождается примерно раз в 1 / speed секунд, раньше спрашивать бессмысленно
        return now() + (1000 / speed).toLong()
    }
}