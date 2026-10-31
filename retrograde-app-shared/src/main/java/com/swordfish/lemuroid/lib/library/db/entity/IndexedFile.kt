package com.swordfish.lemuroid.lib.library.db.entity

import androidx.room.Entity
import androidx.room.Index
import androidx.room.PrimaryKey

@Entity(
    tableName = "indexed_files",
    indices = [
        Index("uri", unique = true),
        Index("lastIndexedAt"),
    ],
)
data class IndexedFile(
    @PrimaryKey(autoGenerate = true)
    val id: Int = 0,
    val uri: String,
    val name: String,
    val size: Long,
    val lastModified: Long,
    val lastIndexedAt: Long,
    val biosFileName: String? = null,
)
