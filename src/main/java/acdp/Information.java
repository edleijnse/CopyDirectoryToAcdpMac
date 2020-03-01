/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp;

import java.nio.file.Path;

import acdp.exceptions.IOFailureException;
import acdp.tools.Setup;
import acdp.types.Type;
import acdp.types.Type.Scheme;

/**
 * Groups the info interfaces on the level of the database, table, column and
 * store, respectively.
 * <p>
 * Information about the database, a particular table, column or store is
 * read-only and can be as ordinary as the name of the database and as
 * technical or internal as the block size in the fixed length (FL) file space
 * of a table's WR store.
 * <p>
 * There should be no need for clients to implement these interfaces.
 *
 * @author Beat Hoermann
 */
public interface Information {
	/**
	 * The database info comprises some useful information about the database,
	 * including all values from the database layout.
	 * 
	 * @author Beat Hoermann
	 */
	public interface DatabaseInfo {
		/**
		 * The type of a database.
		 * A database is either a WR database or an RO database.
		 * For any details consult section "WR and RO Database" in the
		 * description of the {@link Database} interface.
		 * 
		 * @author Beat Hoermann
		 */
		public enum DBType {
			/**
			 * The WR database type.
			 * For any details consult section "WR and RO Database" in the
			 * description of the {@link Database} interface.
			 */
			WR,
			/**
			 * The RO database type.
			 * For any details consult section "WR and RO Database" in the
			 * description of the {@link Database} interface.
			 */
			RO;
		}
		
		/**
		 * Returns the main file of this database.
		 *
		 * @return The main file of this database, never {@code null}.
		 */
		Path mainFile();
		
		/**
		 * Returns the name of this database.
		 * (The {@link Database#name()} method returns the same value.)
		 * <p>
		 * This property is registered in the database layout.
		 * 
		 * @return The name of this database, never {@code null} and never an
		 *         empty string.
		 */
		String name();
		
		/**
		 * Returns the version of this database.
		 * <p>
		 * This property is registered in the database layout.
		 * 
		 * @return The version of this database or {@code null} if no version is
		 *         defined for this database, never an empty string.
		 */
		String version();
		
		/**
		 * Returns the information whether this database is an RO or WR database.
		 * 
		 * @return The type of this database, never {@code null}.
		 */
		DBType type();
		
		/**
		 * Returns the information whether this database is writable or read-only.
		 * A database is writable if and only if the database is a WR database
		 * and if it is not write protected.
		 * 
		 * @return The boolean value {@code true} if this database is writable,
		 *         {@code false} if it is read-only.
		 */
		boolean isWritable();
		
		/**
		 * Returns the consistency number of this database.
		 * <p>
		 * This property is registered in the database layout.
		 * 
		 * @return The consistency number of this database.
		 */
		int consistencyNumber();
		
		/**
		 * Returns the name of the cipher factory class.
		 * If this method returns {@code null} then the {@link
		 * #cipherFactoryClasspath} and the {@link #cipherChallenge} methods
		 * return {@code null} as well and the {@link #appliesEncryption} and
		 * {@link #encryptsRODatabase} methods both return {@code false}.
		 * <p>
		 * This property is registered in the database layout.
		 * 
		 * @return The name of the cipher factory class, may be {@code null},
		 *         never an empty string.
		 */
		String cipherFactoryClassName();
		
		/**
		 * Returns the path of the directory housing the class file of the cipher
		 * factory class and any depending class files.
		 * <p>
		 * This property is registered in the database layout.
		 * 
		 * @return The described classpath, may be {@code null}, never an empty
		 *         string.
		 */
		String cipherFactoryClasspath();
		
		/**
		 * The cipher challenge.
		 * The cipher challenge is used to ensure that this database's cipher
		 * correctly works with respect to the encrypted persisted data.
		 * <p>
		 * This property is registered in the database layout.
		 * 
		 * @return The cipher challenge or {@code null} if and only if this
		 *         database does not apply encryption.
		 */
		String cipherChallenge();
		
		/**
		 * Returns the information whether this database applies encryption.
		 * 
		 * @return The boolean value {@code true} if and only if this database
		 *         applies encryption.
		 */
		boolean appliesEncryption();
		
		/**
		 * Returns the information whether converting a WR database to an RO
		 * database encrypts data in the RO database.
		 * 
		 * @return The boolean value {@code true} if and only if this database is
		 *         a WR database and converting it to an RO database encrypts
		 *         data in the RO database.
		 */
		boolean encryptsRODatabase();
		
		/**
		 * The recorder file of this database.
		 * The recorder file is used by a WR database to roll back a {@linkplain
		 * Unit unit}.
		 * <p>
		 * This property is registered in the database layout.
		 * 
		 * @return The recorder file or {@code null} if and only if this database
		 *         is an RO database.
		 */
		Path recFile();
		
		/**
		 * Indicates if changes to this database are forced being {@linkplain
		 * java.nio.channels.FileChannel#force materialized} when a unit is
		 * committed to guarantee durability (the "D" in "ACID") even in the
		 * case of a system crash.
		 * If the returned value is {@code true} then data changes are
		 * materialized when a unit is committed and changes to the recorder file
		 * are immediately materialized.
		 * Otherwise, changes to this database, as well as changes to the recorder
		 * file are forced being materialized not until this database is closed.
		 * Depending on the storage device, changes may be earlier materialized
		 * in parts or as a whole.
		 * 
		 * @return The boolean value {@code true} if changes to this database are
		 *         forced being materialized as part of committing a unit, {@code
		 *         false} if changes to this database and to the recorder file are
		 *         forced being materialized not until this database is closed.
		 */
		boolean forceWriteCommit();
		
		/**
		 * Returns the table information of all tables in this database.
		 * <p>
		 * The order in which the table information objects appear in the returned
		 * array is equal to the order in which the tables appear in the database
		 * layout.
		 * 
		 * @return The table information of all tables in this database, never
		 *         {@code null} and never empty.
		 */
		TableInfo[] getTableInfos();
	}
	
	/**
	 * The table info comprises some useful information about a table,
	 * including all values from the table layout.
	 * 
	 * @author Beat Hoermann
	 */
	public interface TableInfo {
		/**
		 * Returns the name of this table.
		 * (The {@link Table#name()} method returns the same value.)
		 * <p>
		 * This property is registered in the table layout.
		 * 
		 * @return The name of this table, never {@code null} and never an empty
		 *         string.
		 */
		String name();
		
		/**
		 * Returns the information for each column of this table.
		 * 
		 * @return The information for each column of this table, never {@code
		 *         null} and never empty.
		 *         The array is sorted according to the order defined by the
		 *         {@linkplain Table table definition}.
		 */
		ColumnInfo[] columnInfos();
		
		/**
		 * Returns the information for the store associated with this table.
		 * It is safe to cast the returned object to the {@link WRStoreInfo}
		 * or {@link ROStoreInfo} interface if the database is a WR or RO
		 * database, respectively.
		 * 
		 * @return The store information, never {@code null}.
		 */
		StoreInfo storeInfo();
	}
	
	/**
	 * The column info comprises some useful information about a column,
	 * including all values from the column sublayout.
	 * 
	 * @author Beat Hoermann
	 */
	public interface ColumnInfo {
		/**
		 * Returns the name of this column.
		 * (The {@link Column#name()} method returns the same value.)
		 * <p>
		 * This property is registered in the column sublayout.
		 * 
		 * @return The name of this column, never {@code null} and never an empty
		 *         string.
		 */
		String name();
		
		/**
		 * Returns the type of this column, that is the type of the values a row
		 * can store in this column.
		 * 
		 * @return The type of this column, never {@code null}.
		 */
		Type type();
		
		/**
		 * Returns the name of the custom column type factory class or {@code
		 * null} if the type of this column is a built-in column type.
		 * If this method returns {@code null} then the {@code
		 * customTypeFactoryClasspath} method returns {@code null} as well.
		 * <p>
		 * This property is registered in the column sublayout.
		 * 
		 * @return The name of the custom column type factory, never an empty
		 *         string.
		 *         This value is {@code null} if and only if the type of this
		 *         column is a built-in column type.
		 */
		String typeFactoryClassName();
		
		/**
		 * Returns the path of the directory housing the class file of the custom
		 * column type factory class and any depending class files or {@code
		 * null}.
		 * <p>
		 * This property is registered in the column sublayout.
		 * 
		 * @return The described classpath, never an empty string.
		 *         This value is {@code null} if the type of this column is a
		 *         built-in column type and it <em>may</em> be {@code null} if
		 *         the type of this column is a custom column type.
		 */
		String typeFactoryClasspath();
		
		/**
		 * Returns the name of the table this column references or {@code null} if
		 * the column does not reference a table, hence, if the type of this
		 * column is neither an instance of {@link acdp.types.RefType} nor of
		 * {@link acdp.types.ArrayOfRefType}.
		 * <p>
		 * This property is registered in the column sublayout.
		 * 
		 * @return The name of the referenced table, never an empty string.
		 *         This value is {@code null} if and only if the column does not
		 *         reference a table.
		 */
		String refdTable();
	}
	
	/**
	 * The super interface of the {@link WRStoreInfo} and {@link ROStoreInfo}
	 * interfaces.
	 * The store information object of a table can be obtained executing {@link
	 * TableInfo#storeInfo table.info().storeInfo()}.
	 * 
	 * @author Beat Hoermann
	 */
	public interface StoreInfo {
	}
	
	/**
	 * The WR store info comprises some useful information about the WR store of
	 * a table of a WR database, including all values from the store sublayout.
	 * 
	 * @author Beat Hoermann
	 */
	public interface WRStoreInfo extends StoreInfo {
		/**
		 * Returns the FL data file, that is the backing file for the table's
		 * fixed length data.
		 * <p>
		 * This property is registered in the store sublayout.
		 * 
		 * @return The FL data file, never {@code null}.
		 */
		Path flDataFile();
		
		/**
		 * Returns the VL data file, that is the backing file for the table's
		 * {@linkplain Scheme#OUTROW outrow} data, provided that this store saves
		 * any outrow data.
		 * <p>
		 * This property is registered in the store sublayout.
		 * 
		 * @return The VL data file or {@code null} if and only if this store has
		 *         no outrow data.
		 */
		Path vlDataFile();
		
		/**
		 * Returns the total size in bytes of the used memory blocks within the
		 * FL file space.
		 * Dividing the result of this method by the {@linkplain #blockSize block
		 * size} returns the number of used FL memory blocks which is equal to the
		 * {@linkplain Table#numberOfRows number of rows} that are stored in the
		 * table associated with this store.
		 * <p>
		 * Note that the result of this method is immediately outdated if
		 * concurrent writes are taking place on the table data stored in this
		 * store.
		 * Use a read zone to prevent concurrent writes.
		 * (Read zones are explained in section "Read Zones" of the {@link
		 * Database} interface description.)
		 * 
		 * @return The total size of the used FL memory blocks in bytes.
		 *         This value is greater than or equal to zero.
		 */
		long flUsed();
		
		/**
		 * Returns the total size in bytes of the unused memory blocks within the
		 * FL file space.
		 * Dividing the result of this method by the {@linkplain #blockSize block
		 * size} returns the number of unused FL memory blocks.
		 * <p>
		 * Unused FL memory blocks are reused when rows are inserted into the
		 * table.
		 * {@linkplain Table#compactFL Compacting the FL file space} removes
		 * unused FL memory blocks.
		 * <p>
		 * Note that the result of this method is immediately outdated if
		 * concurrent writes are taking place on the table data stored in this
		 * store.
		 * Use a read zone to prevent concurrent writes.
		 * (Read zones are explained in section "Read Zones" of the {@link
		 * Database} interface description.)
		 * 
		 * @return The total size of the unused FL memory blocks in bytes.
		 *         This value is greater than or equal to zero.
		 */
		long flUnused();
		
		/**
		 * Returns the total size in bytes of the used memory blocks within the
		 * VL file space.
		 * <p>
		 * Note that the result of this method is immediately outdated if
		 * concurrent writes are taking place on the table data stored in this
		 * store.
		 * Use a read zone to prevent concurrent writes.
		 * (Read zones are explained in section "Read Zones" of the {@link
		 * Database} interface description.)
		 * 
		 * @return The total size of the used VL memory blocks in bytes.
		 *         This value is greater than or equal to zero.
		 *         If this store does not save any {@linkplain Scheme#OUTROW
		 *         outrow} data then this value is zero.
		 */
		long vlUsed();
		
		/**
		 * Returns the total size in bytes of the unused memory blocks within the
		 * VL file space.
		 * <p>
		 * {@linkplain Table#compactVL() Compacting the VL file space} removes
		 * the unused VL memory blocks.
		 * <p>
		 * Note that the result of this method is immediately outdated if
		 * concurrent writes are taking place on the table data stored in this
		 * store.
		 * Use a read zone to prevent concurrent writes.
		 * (Read zones are explained in section "Read Zones" of the {@link
		 * Database} interface description.)
		 * 
		 * @return The total size of the unused VL memory blocks in bytes.
		 *         This value is greater than or equal to zero.
		 *         If this store does not save any {@linkplain Scheme#OUTROW
		 *         outrow} data then this value is zero.
		 */
		long vlUnused();
		
		/**
		 * Returns the number of bytes required for referencing a row in the table
		 * associated with this store.
		 * <p>
		 * See the description of the {@linkplain Setup Setup Tool} for further
		 * information about the {@code nobsRowRef}, {@code nobsOutrowPtr} and
		 * {@code nobsRefCount} properties.
		 * <p>
		 * This property is registered in the store sublayout.
		 * 
		 * @return The value of the {@code nobsRowRef} property, greater than
		 *         or equal to 1 and less than or equal to 8.
		 */
		int nobsRowRef();
		
		/**
		 * Returns the number of bytes required for referencing any {@linkplain
		 * Scheme#OUTROW outrow} data in the table associated with this store.
		 * <p>
		 * See the description of the {@linkplain Setup Setup Tool} for further
		 * information about the {@code nobsRowRef}, {@code nobsOutrowPtr} and
		 * {@code nobsRefCount} properties.
		 * <p>
		 * This property is registered in the store sublayout.
		 * 
		 * @return The value of the {@code nobsOutrowPtr} property, greater than
		 *         or equal to 1 and less than or equal to 8.
		 *         If this store does not save any outrow data then this value is
		 *         zero.
		 */
		int nobsOutrowPtr();
		
		/**
		 * Returns the number of bytes used by the reference counter in the header
		 * of a row in the table associated with this store.
		 * <p>
		 * See the description of the {@linkplain Setup Setup Tool} for further
		 * information about the {@code nobsRowRef}, {@code nobsOutrowPtr} and
		 * {@code nobsRefCount} properties.
		 * <p>
		 * This property is registered in the store sublayout.
		 * 
		 * @return The value of the {@code nobsRefCount} property, greater than
		 *         or equal to 1 and less than or equal to 8.
		 *         If the table associated with this store is not referenced by
		 *         any table of the database then this value is zero.
		 */
		int nobsRefCount();
		
		/**
		 * Computes the highest value of the reference counter over all rows of
		 * the table associated with this store.
		 * <p>
		 * Note that the value returned by this method is always less than or
		 * equal to 256<sup>{@code n}</sup>-1, where {@code n} denotes the value
		 * returned by the {@link #nobsRefCount()} method.
		 * <p>
		 * In case of table data corruption this read-only service operation may
		 * throw an exception of a type different from the one listed below.
		 * If the database is a writable WR database then temporary table data
		 * corruption may be due to concurrent writes.
		 * Invoke this operation inside a <em>read zone</em> or a <em>unit</em>
		 * to ensure that no concurrent writes are taken place in the database
		 * while this operation is being executed.
		 * 
		 * @return The highest value of the reference counter or -1 if the table
		 *         associated with this store is not referenced by any table of
		 *         the database.
		 *         
		 * @throws IOFailureException If an I/O error occurs.
		 */
		long highestRefCount() throws IOFailureException;
		
		/**
		 * Returns the size in bytes of a memory block within the FL file space.
		 * 
		 * @return The size of an FL memory block, greater than zero.
		 */
		int blockSize();
		
		/**
		 * Returns the number of memory blocks within the FL file space (used
		 * and unused ones).
		 * <p>
		 * Note that the result of this method is immediately outdated if
		 * concurrent writes are taking place on the table data stored in this
		 * store.
		 * Use a read zone to prevent concurrent writes.
		 * (Read zones are explained in section "Read Zones" of the {@link
		 * Database} interface description.)
		 * 
		 * @return The number of used and unused FL memory blocks, greater than
		 *         or equal to zero.
		 */
		long numberOfBlocks();
		
		/**
		 * Returns the maximum number of memory blocks within the FL file space
		 * (used and unused ones) the ACDP file space manager can sustain.
		 * This number is determined by the {@link #nobsRowRef} property.
		 * 
		 * @return The maximum number of used an unused FL memory blocks, greater
		 *         than or equal to 255.
		 */
		long maxNumberOfBlocks();
		
		/**
		 * The size in bytes of the VL data file, provided that this store saves
		 * any {@linkplain Scheme#OUTROW outrow} data.
		 * <p>
		 * Note that the result of this method is immediately outdated if
		 * concurrent writes are taking place on the table data stored in this
		 * store.
		 * Use a read zone to prevent concurrent writes.
		 * (Read zones are explained in section "Read Zones" of the {@link
		 * Database} interface description.)
		 * 
		 * @return The size of the VL data file in bytes or -1 if this store does
		 *         not save any outrow data.
		 */
		long sizeOfVlDataFile();
		
		/**
		 * Returns the maximum safe size in bytes of the VL data file, provided
		 * that the store saves any {@linkplain Scheme#OUTROW outrow} data.
		 * <p>
		 * Any VL memory block can be referenced from an FL memory block if the
		 * size of the VL data file does not exceed the value returned by this
		 * method.
		 * <p>
		 * The maximum safe size of the VL data file is directly linked to the
		 * {@link #nobsOutrowPtr} property.
		 * 
		 * @return The maximum safe size of the VL data file in bytes, greater
		 *         than or equal to 256 or -1 if this store does not save any
		 *         outrow data.
		 */
		long maxSafeSizeOfVlDataFile();
	}
	
	/**
	 * The RO store info comprises some useful information about the RO store of
	 * a table of an RO database, including all values from the store sublayout.
	 * 
	 * @author Beat Hoermann
	 */
	public interface ROStoreInfo extends StoreInfo {
		/**
		 * Returns the number of rows in the table.
		 * (The {@link Table#numberOfRows} method returns the same value.)
		 * <p>
		 * This property is registered in the store sublayout.
		 * 
		 * @return The number of rows in the table.
		 *         This value is greater than or equal to zero.
		 */
		int nofRows();
		
		/**
		 * Returns the file position where the table data starts within the RO
		 * database file.
		 * <p>
		 * This property is registered in the store sublayout.
		 * 
		 * @return The file position where the table data starts within the RO
		 *         database file.
		 *         This value is greater than zero.
		 */
		long startData();
		
		/**
		 * Returns the number of bytes of the unpacked table data.
		 * <p>
		 * This property is registered in the store sublayout.
		 * 
		 * @return The number of bytes of the unpacked table data.
		 *         This value is greater than or equal to zero.
		 */
		long dataLength();
		
		/**
		 * Returns the position within the RO database file where the row pointers
		 * of the table start.
		 * <p>
		 * This property is registered in the store sublayout.
		 * 
		 * @return The position within the RO database file where the row pointers
		 *         of the table start.
		 *         This value is greater than zero.
		 */
		long startRowPtrs();
		
		/**
		 * Returns the number of bytes of the byte representation of a row
		 * pointer.
		 * <p>
		 * This property is registered in the store sublayout.
		 * 
		 * @return The number of bytes of the byte representation of a row
		 *         pointer.
		 *         This value is greater than or equal to zero.
		 */
		int nobsRowPtr();
		
		/**
		 * Returns the number of data blocks the table data consists of.
		 * <p>
		 * This property is registered in the store sublayout.
		 * 
		 * @return The number of data blocks, greater than or equal to zero.
		 */
		int nofBlocks();
	}
}
