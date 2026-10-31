package com.swordfish.lemuroid.lib.storage.local

import android.content.Context
import com.swordfish.lemuroid.common.kotlin.calculateCrc32
import com.swordfish.lemuroid.common.kotlin.toStringCRC32
import com.swordfish.lemuroid.lib.storage.BaseStorageFile
import com.swordfish.lemuroid.lib.storage.StorageFile
import com.swordfish.lemuroid.lib.library.GameSystem
import com.swordfish.lemuroid.lib.storage.scanner.SerialScanner
import timber.log.Timber
import java.io.FilterInputStream
import java.io.InputStream
import java.util.zip.ZipEntry
import java.util.zip.ZipInputStream

object DocumentFileParser {
    private const val MAX_CHECKED_ENTRIES = 3
    private const val SINGLE_ARCHIVE_THRESHOLD = 0.9
    private const val MAX_SIZE_CRC32 = 1_000_000_000

    fun parseDocumentFile(
        context: Context,
        baseStorageFile: BaseStorageFile,
        calculateCrc: Boolean = true,
    ): StorageFile? {
        if (!isPotentialGameFile(baseStorageFile)) return null

        return if (baseStorageFile.extension.lowercase() == "zip") {
            Timber.d("Detected zip file. ${baseStorageFile.name}")
            parseZipFile(context, baseStorageFile, calculateCrc)
        } else {
            Timber.d("Detected standard file. ${baseStorageFile.name}")
            parseStandardFile(context, baseStorageFile, calculateCrc)
        }
    }

    private fun parseZipFile(
        context: Context,
        baseStorageFile: BaseStorageFile,
        calculateCrc: Boolean,
    ): StorageFile {
        var bytesRead = 0L
        val inputStream =
            context.contentResolver.openInputStream(baseStorageFile.uri)
                ?.let { CountingInputStream(it) { bytesRead += it } }
        return ZipInputStream(inputStream).use {
            val gameEntry = findGameEntry(it, baseStorageFile.size)
            if (gameEntry != null) {
                Timber.d("Handing zip file as compressed game: ${baseStorageFile.name}")
                parseCompressedGame(baseStorageFile, gameEntry, it) { bytesRead }
            } else {
                Timber.d("Handing zip file as standard: ${baseStorageFile.name}")
                parseStandardFile(context, baseStorageFile, calculateCrc)
            }
        }
    }

    private fun parseCompressedGame(
        baseStorageFile: BaseStorageFile,
        entry: ZipEntry,
        zipInputStream: ZipInputStream,
        bytesRead: () -> Long,
    ): StorageFile {
        Timber.d("Processing zipped entry: ${entry.name}")

        val diskInfo = SerialScanner.extractInfo(entry.name, zipInputStream)

        return StorageFile(
            entry.name,
            entry.size,
            entry.crc.toStringCRC32(),
            diskInfo.serial,
            baseStorageFile.uri,
            baseStorageFile.uri.path,
            diskInfo.systemID,
            bytesRead = bytesRead(),
        )
    }

    private fun parseStandardFile(
        context: Context,
        baseStorageFile: BaseStorageFile,
        calculateCrc: Boolean,
    ): StorageFile {
        var bytesRead = 0L

        val diskInfo =
        context.contentResolver.openInputStream(baseStorageFile.uri)
            ?.let { inputStream ->
                SerialScanner.extractInfo(
                    baseStorageFile.name,
                    CountingInputStream(inputStream) { bytesRead += it },
                )
            }

        val crc32 =
        if (calculateCrc && baseStorageFile.size < MAX_SIZE_CRC32 && diskInfo?.serial == null) {
            context.contentResolver.openInputStream(baseStorageFile.uri)
                ?.let { calculateCrc32FromStream(CountingInputStream(it) { bytesRead += it }) }
        } else {
            null
        }

        Timber.d("Parsed standard file: $baseStorageFile")

        return StorageFile(
            baseStorageFile.name,
            baseStorageFile.size,
            crc32,
            diskInfo?.serial,
            baseStorageFile.uri,
            baseStorageFile.uri.path,
            diskInfo?.systemID,
            bytesRead = bytesRead,
        )
    }

    private fun calculateCrc32FromStream(inputStream: InputStream): String {
        return inputStream.calculateCrc32()
    }

    private fun isPotentialGameFile(baseStorageFile: BaseStorageFile): Boolean {
        val extension = baseStorageFile.extension.lowercase()
        return extension.isBlank() ||
            extension in SUPPORTED_EXTENSIONS ||
            extension in BIOS_EXTENSIONS
    }

    private class CountingInputStream(
        inputStream: InputStream,
        private val onBytesRead: (Long) -> Unit,
    ) : FilterInputStream(inputStream) {
        var bytesRead = 0L

        override fun read(): Int {
            val result = super.read()
            if (result >= 0) {
                bytesRead++
                onBytesRead(1)
            }
            return result
        }

        override fun read(
            buffer: ByteArray,
            offset: Int,
            length: Int,
        ): Int {
            val result = super.read(buffer, offset, length)
            if (result > 0) {
                bytesRead += result
                onBytesRead(result.toLong())
            }
            return result
        }
    }

    private val BIOS_EXTENSIONS = setOf("bin", "img")
    private val SUPPORTED_EXTENSIONS = GameSystem.getSupportedExtensions().map { it.lowercase() }.toSet()

    /* Finds a zip entry which we assume is a game. Lemuroid only supports single archive games,
       so we are looking for an entry which occupies a large percentage of the archive space.
       This is very fast heuristic to compute and avoids reading the whole stream in most
       scenarios.*/
    fun findGameEntry(
        openedInputStream: ZipInputStream,
        fileSize: Long = -1,
    ): ZipEntry? {
        for (i in 0..MAX_CHECKED_ENTRIES) {
            val entry = openedInputStream.nextEntry ?: break
            if (!isGameEntry(entry, fileSize)) continue
            return entry
        }
        return null
    }

    private fun isGameEntry(
        entry: ZipEntry,
        fileSize: Long,
    ): Boolean {
        if (fileSize <= 0 || entry.compressedSize <= 0) return false
        return (entry.compressedSize.toFloat() / fileSize.toFloat()) > SINGLE_ARCHIVE_THRESHOLD
    }
}
