package ru.quipy.common.utils

import kotlin.math.ceil

/**
 * Leaky bucket в режиме meter: каждый пропущенный запрос доливает в ведро 1,
 * ведро непрерывно вытекает со скоростью leakPerSec. Места нет — запрос не пропускаем.
 * Размер ведра передаётся в каждый вызов: у нас он зависит от дедлайна заказа.
 */
class LeakyBucketMeter(private val leakPerSec: Double) {
    private var level = 0.0
    private var updatedAt = System.currentTimeMillis()

    // null — пропустили (долили 1); иначе — через сколько мс в ведре освободится место
    @Synchronized
    fun tryAdd(size: Double): Long? {
        val now = System.currentTimeMillis()
        level = maxOf(0.0, level - (now - updatedAt) * leakPerSec / 1000)
        updatedAt = now
        if (level + 1 <= size) {
            level += 1
            return null
        }
        return ceil((level + 1 - size) * 1000 / leakPerSec).toLong()
    }
}
