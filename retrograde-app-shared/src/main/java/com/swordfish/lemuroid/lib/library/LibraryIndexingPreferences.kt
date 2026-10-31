package com.swordfish.lemuroid.lib.library

import android.content.Context
import com.swordfish.lemuroid.lib.preferences.SharedPreferencesHelper

class LibraryIndexingPreferences(private val context: Context) {
    private val preferences = SharedPreferencesHelper.getSharedPreferences(context)

    val incrementalIndexingEnabled: Boolean
        get() = preferences.getBoolean(
            context.getString(com.swordfish.lemuroid.lib.R.string.pref_key_incremental_indexing),
            true,
        )

    val parallelPipelineEnabled: Boolean
        get() = preferences.getBoolean(
            context.getString(com.swordfish.lemuroid.lib.R.string.pref_key_parallel_indexing),
            true,
        )

    val reduceExpensiveReadsEnabled: Boolean
        get() = preferences.getBoolean(
            context.getString(com.swordfish.lemuroid.lib.R.string.pref_key_reduce_expensive_reads),
            true,
        )

    val workerCount: Int
        get() =
            preferences
                .getString(
                    context.getString(com.swordfish.lemuroid.lib.R.string.pref_key_indexing_workers),
                    DEFAULT_WORKER_COUNT.toString(),
                )
                ?.toIntOrNull()
                ?.coerceIn(MIN_WORKERS, MAX_WORKERS)
                ?: DEFAULT_WORKER_COUNT

    companion object {
        private const val MIN_WORKERS = 1
        private const val MAX_WORKERS = 8
        private const val DEFAULT_WORKER_COUNT = 4
    }
}
