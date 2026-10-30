/*
 * GameLibrary.kt
 *
 * Copyright (C) 2017 Retrograde Project
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */

package com.swordfish.lemuroid.lib.library

import com.swordfish.lemuroid.common.coroutines.batchWithSizeAndTime
import com.swordfish.lemuroid.lib.bios.BiosManager
import com.swordfish.lemuroid.lib.library.covers.CustomCoverManager
import com.swordfish.lemuroid.lib.library.db.RetrogradeDatabase
import com.swordfish.lemuroid.lib.library.db.entity.DataFile
import com.swordfish.lemuroid.lib.library.db.entity.Game
import com.swordfish.lemuroid.lib.library.db.entity.IndexedFile
import com.swordfish.lemuroid.lib.library.metadata.GameMetadata
import com.swordfish.lemuroid.lib.library.metadata.GameMetadataProvider
import com.swordfish.lemuroid.lib.storage.BaseStorageFile
import com.swordfish.lemuroid.lib.storage.GroupedStorageFiles
import com.swordfish.lemuroid.lib.storage.RomFiles
import com.swordfish.lemuroid.lib.storage.StorageFile
import com.swordfish.lemuroid.lib.storage.StorageProvider
import com.swordfish.lemuroid.lib.storage.StorageProviderRegistry
import dagger.Lazy
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flatMapConcat
import kotlinx.coroutines.flow.flatMapMerge
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.onEach
import timber.log.Timber

class LemuroidLibrary(
    private val retrogradedb: RetrogradeDatabase,
    private val storageProviderRegistry: Lazy<StorageProviderRegistry>,
    private val gameMetadataProvider: Lazy<GameMetadataProvider>,
    private val biosManager: BiosManager,
    private val customCoverManager: CustomCoverManager,
    private val indexingPreferences: LibraryIndexingPreferences,
) {
    private val metadataCache = ConcurrentHashMap<String, GameMetadata>()

    suspend fun indexLibrary() {
        val startedAtMs = System.currentTimeMillis()
        val metrics = ScanMetrics()
        metadataCache.clear()

        try {
            indexProviders(startedAtMs, metrics)
        } catch (e: Throwable) {
            Timber.e(e, "Library indexing stopped due to exception")
        } finally {
            cleanUp(startedAtMs)
        }

        val executionTime = System.currentTimeMillis() - startedAtMs
        Timber.i(
            "Library indexing completed in: ${executionTime} ms " +
                "(enumerated=${metrics.enumeratedFiles}, analyzed=${metrics.analyzedFiles}, " +
                "reused=${metrics.reusedFiles}, metadata=${metrics.metadataLookups}, " +
                "bytesRead=${metrics.bytesRead}, metadataMs=${metrics.metadataTimeMs}, " +
                "persistenceMs=${metrics.persistenceTimeMs})",
        )
    }

    @OptIn(FlowPreview::class)
    private suspend fun indexProviders(
        startedAtMs: Long,
        metrics: ScanMetrics,
    ) {
        val gameMetadata = gameMetadataProvider.get()
        val enabledProviders = storageProviderRegistry.get().enabledProviders
        enabledProviders.asFlow()
            .flatMapConcat { indexSingleProvider(it, startedAtMs, gameMetadata, metrics) }
            .collect()
    }

    @OptIn(FlowPreview::class)
    private fun indexSingleProvider(
        provider: StorageProvider,
        startedAtMs: Long,
        gameMetadata: GameMetadataProvider,
        metrics: ScanMetrics,
    ): Flow<Unit> {
        val batches =
            provider.listBaseStorageFiles()
                .onEach { metrics.enumeratedFiles.addAndGet(it.size.toLong()) }
                .flatMapConcat { StorageFilesMerger.mergeDataFiles(provider, it).asFlow() }
                .batchWithSizeAndTime(MAX_BUFFER_SIZE, MAX_TIME)

        return if (indexingPreferences.parallelPipelineEnabled) {
            batches.flatMapMerge(concurrency = indexingPreferences.workerCount) {
                processBatch(it, provider, startedAtMs, gameMetadata, metrics)
            }
        } else {
            batches.flatMapConcat {
                processBatch(it, provider, startedAtMs, gameMetadata, metrics)
            }
        }
    }

    private suspend fun processBatch(
        batch: List<GroupedStorageFiles>,
        provider: StorageProvider,
        startedAtMs: Long,
        gameMetadata: GameMetadataProvider,
        metrics: ScanMetrics,
    ) = flow<Unit> {
        val entries = batch.map { fetchEntriesFromDatabase(it, metrics) }
        val cachedEntries = entries.filterIsInstance<ScanEntry.Cached>()
        val resolvedEntries =
            entries
                .filterIsInstance<ScanEntry.File>()
                .map { buildEntryFromMetadata(it, provider, gameMetadata, startedAtMs, metrics) }

        val existingEntries =
            cachedEntries.mapNotNull { cached ->
                cached.game?.let { ScanEntry.GameFile(cached.file, it.copy(lastIndexedAt = startedAtMs)) }
            } +
                resolvedEntries.filterIsInstance<ScanEntry.GameFile>().filter { it.game.id != 0 }
        val newEntries =
            resolvedEntries
                .filterIsInstance<ScanEntry.GameFile>()
                .filter { it.game.id == 0 }

        val persistenceStartedAt = System.currentTimeMillis()
        handleExistingEntries(existingEntries, startedAtMs)
        handleNewGames(newEntries, startedAtMs)

        val unknownEntries =
            resolvedEntries
                .filterIsInstance<ScanEntry.File>()
                .filter { it.existingGame == null }
        val biosNames = handleUnknownFiles(provider, unknownEntries, startedAtMs, metrics)
        persistIndexedFiles(batch, cachedEntries, biosNames, startedAtMs)
        metrics.persistenceTimeMs.addAndGet(System.currentTimeMillis() - persistenceStartedAt)
    }

    private fun fetchEntriesFromDatabase(
        storageFile: GroupedStorageFiles,
        metrics: ScanMetrics,
    ): ScanEntry {
        Timber.d("Retrieving scan entry for uri: ${storageFile.primaryFile.uri}")
        val game = retrogradedb.gameDao().selectByFileUri(storageFile.primaryFile.uri.toString())
        val cachedFiles =
            storageFile.allFiles().mapNotNull { baseFile ->
                retrogradedb.indexedFileDao().selectByUri(baseFile.uri.toString())
            }
        val unchanged =
            indexingPreferences.incrementalIndexingEnabled &&
                storageFile.allFiles().all { baseFile ->
                    cachedFiles.firstOrNull { it.uri == baseFile.uri.toString() }?.matches(baseFile) == true
                }

        if (game != null && (!indexingPreferences.incrementalIndexingEnabled || unchanged) ||
            game == null && unchanged
        ) {
            metrics.reusedFiles.addAndGet(storageFile.allFiles().size.toLong())
            return ScanEntry.Cached(storageFile, game, cachedFiles)
        }

        return ScanEntry.File(storageFile, game)
    }

    private fun IndexedFile.matches(baseStorageFile: BaseStorageFile): Boolean {
        return uri == baseStorageFile.uri.toString() &&
            size == baseStorageFile.size &&
            lastModified == baseStorageFile.lastModified
    }

    private suspend fun buildEntryFromMetadata(
        entry: ScanEntry.File,
        provider: StorageProvider,
        metadataProvider: GameMetadataProvider,
        startedAtMs: Long,
        metrics: ScanMetrics,
    ): ScanEntry {
        val metadataStartedAt = System.currentTimeMillis()
        val game =
            sortedFilesForScanning(entry.file).asSequence()
                .mapNotNull { baseFile ->
                    val fastStorageFile = safeStorageFile(provider, baseFile, !indexingPreferences.reduceExpensiveReadsEnabled, metrics)
                    if (fastStorageFile == null) {
                        null
                    } else {
                        val fastMetadata = retrieveMetadata(metadataProvider, fastStorageFile, metrics)
                        if (fastMetadata != null) {
                            convertGameMetadataToGame(entry.file, fastStorageFile, fastMetadata, startedAtMs, entry.existingGame)
                        } else if (
                            indexingPreferences.reduceExpensiveReadsEnabled &&
                            fastStorageFile.crc == null &&
                            fastStorageFile.size < MAX_SIZE_CRC32
                        ) {
                            safeStorageFile(provider, baseFile, true, metrics)
                                ?.let { storageFile ->
                                    retrieveMetadata(metadataProvider, storageFile, metrics)
                                        ?.let {
                                            convertGameMetadataToGame(
                                                entry.file,
                                                storageFile,
                                                it,
                                                startedAtMs,
                                                entry.existingGame,
                                            )
                                        }
                                }
                        } else {
                            null
                        }
                    }
                }
                .firstOrNull()

        metrics.metadataTimeMs.addAndGet(System.currentTimeMillis() - metadataStartedAt)
        return when {
            game != null -> ScanEntry.GameFile(entry.file, game)
            entry.existingGame != null ->
                ScanEntry.GameFile(entry.file, entry.existingGame.copy(lastIndexedAt = startedAtMs))
            else -> entry
        }
    }

    private suspend fun retrieveMetadata(
        metadataProvider: GameMetadataProvider,
        storageFile: StorageFile,
        metrics: ScanMetrics,
    ): GameMetadata? {
        val keys =
            listOf(
                storageFile.crc?.let { "crc:$it" },
                storageFile.serial?.let { "serial:$it" },
                "name:${storageFile.name}",
                storageFile.path?.let { "path:$it/name:${storageFile.name}" },
            ).filterNotNull()

        keys.firstNotNullOfOrNull { metadataCache[it] }?.let { return it }

        metrics.metadataLookups.incrementAndGet()
        val metadata = metadataProvider.retrieveMetadata(storageFile)
        if (metadata != null) {
            keys.forEach { metadataCache[it] = metadata }
        }
        return metadata
    }

    private fun handleExistingEntries(
        entries: List<ScanEntry.GameFile>,
        startedAtMs: Long,
    ) {
        if (entries.isEmpty()) return
        updateGames(entries, startedAtMs)
        updateDataFiles(entries, startedAtMs)
    }

    private fun updateGames(
        entries: List<ScanEntry.GameFile>,
        startedAtMs: Long,
    ) {
        val updatedGames = entries.map { it.game.copy(lastIndexedAt = startedAtMs) }
        updatedGames.forEach { Timber.d("Updating game: $it") }
        retrogradedb.gameDao().update(updatedGames)
    }

    private fun updateDataFiles(
        entries: List<ScanEntry.GameFile>,
        startedAtMs: Long,
    ) {
        val dataFileDao = retrogradedb.dataFileDao()
        entries.forEach { entry ->
            val existingFiles = dataFileDao.selectDataFilesForGame(entry.game.id)
            val existingByUri = existingFiles.associateBy { it.fileUri }
            val currentFiles =
                entry.file.dataFiles.map { baseFile ->
                    convertIntoDataFile(
                        entry.game.id,
                        baseFile,
                        startedAtMs,
                        existingByUri[baseFile.uri.toString()],
                    )
                }
            val currentUris = currentFiles.map { it.fileUri }.toSet()
            val removedFiles = existingFiles.filterNot { it.fileUri in currentUris }
            if (removedFiles.isNotEmpty()) dataFileDao.delete(removedFiles)

            val updatedFiles = currentFiles.filter { it.id != 0 }
            val newFiles = currentFiles.filter { it.id == 0 }
            if (updatedFiles.isNotEmpty()) dataFileDao.update(updatedFiles)
            if (newFiles.isNotEmpty()) dataFileDao.insert(newFiles)
        }
    }

    private fun convertIntoDataFile(
        gameId: Int,
        baseStorageFile: BaseStorageFile,
        startedAtMs: Long,
        existingFile: DataFile? = null,
    ): DataFile {
        return DataFile(
            id = existingFile?.id ?: 0,
            gameId = gameId,
            fileUri = baseStorageFile.uri.toString(),
            fileName = baseStorageFile.name,
            lastIndexedAt = startedAtMs,
            path = baseStorageFile.path,
        )
    }

    private fun handleNewGames(
        entries: List<ScanEntry.GameFile>,
        startedAtMs: Long,
    ) {
        if (entries.isEmpty()) return
        val games = entries.map { it.game }
        games.forEach { Timber.d("Insert: $it") }

        val gameIds = retrogradedb.gameDao().insert(games)
        val dataFiles =
            entries
                .map { it.file.dataFiles }
                .zip(gameIds)
                .flatMap { (files, gameId) ->
                    files.map { convertIntoDataFile(gameId.toInt(), it, startedAtMs) }
                }
        if (dataFiles.isNotEmpty()) retrogradedb.dataFileDao().insert(dataFiles)
    }

    private fun handleUnknownFiles(
        provider: StorageProvider,
        entries: List<ScanEntry.File>,
        startedAtMs: Long,
        metrics: ScanMetrics,
    ): Map<String, String> {
        val biosNames = mutableMapOf<String, String>()
        entries
            .flatMap { it.file.allFiles() }
            .forEach { baseStorageFile ->
                val fastStorageFile = safeStorageFile(provider, baseStorageFile, false, metrics) ?: return@forEach
                var biosName = biosManager.findBiosFileName(fastStorageFile)
                val storageFile =
                    if (biosName == null && indexingPreferences.reduceExpensiveReadsEnabled) {
                        safeStorageFile(provider, baseStorageFile, true, metrics)
                    } else {
                        fastStorageFile
                    } ?: return@forEach

                biosName = biosManager.findBiosFileName(storageFile)
                if (biosName != null) {
                    provider.getInputStream(storageFile.uri)?.let { inputStream ->
                        biosManager.tryAddBiosAfter(storageFile, inputStream, startedAtMs)
                        biosNames[baseStorageFile.uri.toString()] = biosName
                    }
                }
            }
        return biosNames
    }

    private fun persistIndexedFiles(
        batch: List<GroupedStorageFiles>,
        cachedEntries: List<ScanEntry.Cached>,
        biosNames: Map<String, String>,
        startedAtMs: Long,
    ) {
        val cachedByUri =
            cachedEntries
                .flatMap { it.indexedFiles }
                .associateBy { it.uri }
        val indexedFiles =
            batch
                .flatMap { it.allFiles() }
                .distinctBy { it.uri }
                .map { baseFile ->
                    val uri = baseFile.uri.toString()
                    IndexedFile(
                        id = cachedByUri[uri]?.id ?: 0,
                        uri = uri,
                        name = baseFile.name,
                        size = baseFile.size,
                        lastModified = baseFile.lastModified,
                        lastIndexedAt = startedAtMs,
                        biosFileName = biosNames[uri] ?: cachedByUri[uri]?.biosFileName,
                    )
                }
        if (indexedFiles.isNotEmpty()) retrogradedb.indexedFileDao().insert(indexedFiles)
        cachedEntries
            .flatMap { it.indexedFiles }
            .filter { it.biosFileName != null && it.uri !in biosNames }
            .forEach { biosManager.touchBios(it.biosFileName!!, startedAtMs) }
    }

    private fun safeStorageFile(
        provider: StorageProvider,
        baseStorageFile: BaseStorageFile,
        calculateCrc: Boolean,
        metrics: ScanMetrics,
    ): StorageFile? {
        return runCatching {
            provider.getStorageFile(baseStorageFile, calculateCrc)
        }.getOrNull()?.also {
            metrics.analyzedFiles.incrementAndGet()
            metrics.bytesRead.addAndGet(it.bytesRead)
        }
    }

    private fun cleanUp(startedAtMs: Long) {
        runCatching { removeDeletedBios(startedAtMs) }
        runCatching { removeDeletedGames(startedAtMs) }
        runCatching { removeDeletedDataFiles(startedAtMs) }
        runCatching { removeDeletedIndexedFiles(startedAtMs) }
    }

    private fun removeDeletedBios(startedAtMs: Long) {
        biosManager.deleteBiosBefore(startedAtMs)
    }

    private fun sortedFilesForScanning(groupedStorageFile: GroupedStorageFiles): List<BaseStorageFile> {
        return groupedStorageFile.dataFiles.sortedBy { it.name } + listOf(groupedStorageFile.primaryFile)
    }

    private fun convertGameMetadataToGame(
        groupedStorageFile: GroupedStorageFiles,
        storageFile: StorageFile,
        gameMetadata: GameMetadata,
        lastIndexedAt: Long,
        existingGame: Game?,
    ): Game {
        val gameSystem = GameSystem.findById(gameMetadata.system!!)
        val fileName =
            if (groupedStorageFile.dataFiles.isNotEmpty()) groupedStorageFile.primaryFile.name else storageFile.name

        return Game(
            id = existingGame?.id ?: 0,
            fileName = fileName,
            fileUri = groupedStorageFile.primaryFile.uri.toString(),
            title = gameMetadata.name ?: groupedStorageFile.primaryFile.name,
            systemId = gameSystem.id.dbname,
            developer = gameMetadata.developer,
            coverFrontUrl = gameMetadata.thumbnail,
            lastIndexedAt = lastIndexedAt,
            lastPlayedAt = existingGame?.lastPlayedAt,
            isFavorite = existingGame?.isFavorite ?: false,
            customName = existingGame?.customName,
            customCoverUri = existingGame?.customCoverUri,
        )
    }

    private fun removeDeletedDataFiles(startedAtMs: Long) {
        Timber.d("Deleting data files from db before: $startedAtMs")
        val dataFiles = retrogradedb.dataFileDao().selectByLastIndexedAtLessThan(startedAtMs)
        if (dataFiles.isNotEmpty()) retrogradedb.dataFileDao().delete(dataFiles)
    }

    private fun removeDeletedGames(startedAtMs: Long) {
        Timber.d("Deleting games from db before: $startedAtMs")
        val games = retrogradedb.gameDao().selectByLastIndexedAtLessThan(startedAtMs)
        if (games.isEmpty()) return
        customCoverManager.deleteCoversForGames(games)
        retrogradedb.gameDao().delete(games)
    }

    private fun removeDeletedIndexedFiles(startedAtMs: Long) {
        val indexedFiles = retrogradedb.indexedFileDao().selectByLastIndexedAtLessThan(startedAtMs)
        val ids = indexedFiles.map { it.id }
        if (ids.isNotEmpty()) retrogradedb.indexedFileDao().deleteByIds(ids)
    }

    fun getGameFiles(
        game: Game,
        dataFiles: List<DataFile>,
        allowVirtualFiles: Boolean,
    ): RomFiles {
        val provider = storageProviderRegistry.get()
        return provider.getProvider(game).getGameRomFiles(game, dataFiles, allowVirtualFiles)
    }

    private sealed class ScanEntry {
        data class GameFile(val file: GroupedStorageFiles, val game: Game) : ScanEntry()

        data class File(val file: GroupedStorageFiles, val existingGame: Game?) : ScanEntry()

        data class Cached(
            val file: GroupedStorageFiles,
            val game: Game?,
            val indexedFiles: List<IndexedFile>,
        ) : ScanEntry()
    }

    private class ScanMetrics {
        val enumeratedFiles = AtomicLong()
        val analyzedFiles = AtomicLong()
        val reusedFiles = AtomicLong()
        val metadataLookups = AtomicLong()
        val bytesRead = AtomicLong()
        val metadataTimeMs = AtomicLong()
        val persistenceTimeMs = AtomicLong()
    }

    companion object {
        const val MAX_BUFFER_SIZE = 200
        const val MAX_TIME = 5000
        private const val MAX_SIZE_CRC32 = 1_000_000_000
    }
}
