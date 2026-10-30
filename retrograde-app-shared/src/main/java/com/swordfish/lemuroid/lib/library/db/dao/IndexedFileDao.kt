package com.swordfish.lemuroid.lib.library.db.dao

import androidx.room.Dao
import androidx.room.Insert
import androidx.room.OnConflictStrategy
import androidx.room.Query
import com.swordfish.lemuroid.lib.library.db.entity.IndexedFile

@Dao
interface IndexedFileDao {
    @Query("SELECT * FROM indexed_files WHERE uri = :uri")
    fun selectByUri(uri: String): IndexedFile?

    @Query("SELECT * FROM indexed_files WHERE lastIndexedAt < :lastIndexedAt")
    fun selectByLastIndexedAtLessThan(lastIndexedAt: Long): List<IndexedFile>

    @Insert(onConflict = OnConflictStrategy.REPLACE)
    fun insert(files: List<IndexedFile>)

    @Query("DELETE FROM indexed_files WHERE id IN (:ids)")
    fun deleteByIds(ids: List<Int>)
}
