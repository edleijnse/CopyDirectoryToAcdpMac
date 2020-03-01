/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.tools;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.nio.file.Path;
import java.nio.file.Paths;

import acdp.Database;
import acdp.Ref;
import acdp.design.CustomDatabase;
import acdp.design.CustomTable;
import acdp.design.ICipherFactory;
import acdp.design.SimpleType;
import acdp.exceptions.IOFailureException;
import acdp.exceptions.MaximumException;
import acdp.internal.FileIOException;
import acdp.internal.SetupTool;
import acdp.types.Type.Scheme;

/**
 * Assists with setting up a database once the concrete custom database class,
 * the concrete custom table classes, the optional custom column types and the
 * optional cipher factory class have been coded.
 * All generated files, among them the database's layout file, are saved into a
 * single directory.
 * As a side effect, the Setup Tool checks the coded classes for correctness
 * and writes a meaningful message to the {@linkplain System#out standard output
 * stream} in the case of an error.
 * <p>
 * The Setup Tool needs as input the name of the <em>database class</em>, that
 * is, a concrete subclass of the {@link CustomDatabase} class.
 * In order for the Setup Tool to be able to process the database class, the
 * database class and the table classes must be annotated as described in
 * the descriptions of the {@code CustomDatabase} and the {@link CustomTable}
 * classes.
 * Needless to say that such annotations are only required for setting up the
 * database.
 * They can be removed once the database is setup.
 * <p>
 * Although the generated database layout, containing the nested layouts of the
 * tables and columns, is complete and can be immediately used to open the
 * database, you may want to manually customize it to suit your personal needs,
 * for example for adding some comments (comment lines in a {@linkplain
 * acdp.misc.Layout layout} begin with the number sign ('#') character), for
 * renaming or relocating the generated backing files of the database or for
 * reducing the values of the table-specific settings {@code nobsRowRef},
 * {@code nobsOutrowPtr} and {@code nobsRefCount} to get a more compact storage
 * format.
 * (For an explanation of these fields read chapter "Explanation of the {@code
 * nobsRowRef}, {@code nobsOutrowPtr} and {@code nobsRefCount} Settings" at
 * the bottom.)
 * Of course, all this can also be done later when some data has made its way
 * into the database, but then no longer manually because there is a risk of
 * data integrity violation.
 * Use the methods defined in the {@link Settings} class to view all database
 * settings stored in the layout file and to modify some of them or {@linkplain
 * Refactor refactor} the database to safely change the structure of the
 * database.
 * <p>
 * If a table has a custom column type (contrary to a built-in column type)
 * then its {@linkplain SimpleType.TypeFromDesc annotated }type factory method
 * is assumed to be a member of the class defining that custom column type.
 * If this is not the case then please change the value of the {@code
 * typeFactoryClassName} field of the corresponding column sublayout
 * accordingly.
 * On this occasion, you may want to add the {@code typeFactoryClasspath} field
 * in that  column sublayout with a value equal to the directory housing the
 * class file of the class that defines the type factory class method and any
 * depending class files.
 * Of course, this is only necessary if those class files are not yet on
 * the classpath which may be the case if the database is opened as a
 * {@linkplain Database weakly typed database}.
 * (You can also put your class files inside a JAR file and save it into the
 * directory specified by the {@code typeFactoryClasspath} layout field or into
 * any subdirectory of this directory.)
 * <p>
 * Also note that if a database applies encryption then there must be a class
 * declared with a {@code public} access level modifier on the classpath
 * implementing the {@link ICipherFactory} interface.
 * The Setup Tool assumes the name of such a class to be equal to {@code
 * CipherFactory} and to reside as a static nested class inside the database
 * class.
 * If this assumption is not correct then set the {@code cipherFactoryClassName}
 * element of the {@code @Setup_Database} annotation to the proper value.
 * Anyway, the Setup Tool checks if the cipher factory works properly and adds
 * the class name to the database layout under the label {@code
 * cipherFactoryClassName}.
 * Similarly to the {@code typeFactoryClasspath} field from above, an extra
 * classpath for the cipher factory class may be required.
 * It can be specified in the {@code cipherFactoryClasspath} element of the
 * {@code @Setup_Database} annotation from where it is copied to the database
 * layout under the label {@code cipherFactoryClasspath}.
 * <p>
 * If the database applies encryption then the Setup Tool computes the cipher
 * challenge and adds it to the database layout under the label {@code
 * cipherChallenge}.
 * (The cipher challenge is used by ACDP to find out if the cipher created by
 * the registered cipher factory properly works before accessing any encrypted
 * data stored in the database.)
 * For information, the cipher challenge is written to the standard output
 * stream.
 * <p>
 * The Setup Tool can be run from the {@link #main} method or by invoking the
 * {@link #run} method.
 * 
 * <h1>Explanation of the {@code nobsRowRef}, {@code nobsOutrowPtr} and {@code
 * nobsRefCount} Settings</h1>
 * Each table is roughly sized by the values of the following three parameters
 * which appear in the {@code store} section of the table layout: The {@code
 * nobsRowRef}, {@code nobsOutrowPtr} and {@code nobsRefCount} parameters.
 * (The abbreviation {@code nobs} stands for "number of bytes".)
 * 
 * <h2>{@code nobsRowRef}</h2>
 * The value of the {@code nobsRowRef} parameter limits the number of rows in
 * the table to 256<sup>{@code nobsRowRef}</sup> - 1, e.g., 65535 if {@code
 * nobsRowRef} is equal to 2.
 * The value of {@code nobsRowRef} cannot be greater than 8 and if it is 8 then
 * the size of the table is limited to the absolute maximum of {@link
 * Long#MAX_VALUE} &asymp; 9&sdot;10<sup>18</sup> rows.
 * (In practice, this is not a limitation if we keep in mind that a file cannot
 * be larger than {@code Long.MAX_VALUE} bytes.)
 * The size of the byte representation of a {@linkplain Ref reference} is
 * identical to the value of this parameter.
 * 
 * <h2>{@code nobsOutrowPtr}</h2>
 * Table data from columns with an {@linkplain Scheme#OUTROW outrow} storage
 * scheme or being of an {@linkplain Scheme#INROW inrow}  array type having an
 * outrow element type is stored in a separate file, the so called "VL data
 * file".
 * Outrow data is referenced by a <em>file position</em> that locates the start
 * of the outrow data within the VL data file.
 * The file position is an unsigned integer of length equal to the value of the
 * {@code nobsOutrowPtr} parameter thus limiting the size of the VL data file
 * by the size of all blocks with a file position less than
 * 256<sup>{@code nobsOutrowPtr}</sup> - 1.
 * As with the {@code nobsRowRef} parameter, the value of {@code nobsOutrowPtr}
 * cannot be greater than 8 and if it is 8 then the size of the file storing
 * the outrow data of the table is limited to the absolute maximum of {@code
 * Long.MAX_VALUE} bytes which is around eight million terabytes.
 * This parameter is missing if the table doesn't store any outrow data.
 * 
 * <h2>{@code nobsRefCount}</h2>
 * The value of the {@code nobsRefCount} parameter limits the number of rows
 * that can reference a particular row within the table to
 * 256<sup>{@code nobsRefCount}</sup>-1, e.g., 255 if {@code nobsRefCount} is
 * equal to 1.
 * As with the other two fields, the value of {@code nobsRefCount} cannot be
 * greater than 8 and if it is 8 then the number of rows referencing a
 * particular row within the table is limited to the absolute maximum of {@code
 * Long.MAX_VALUE} &asymp; 9&sdot;10<sup>18</sup> rows.
 * This parameter is missing if none of the tables of the database (including
 * this table) has a column referencing this table.
 * <p>
 * The Setup Tool sets these three parameters to their maximum value of 8.
 * As long as the table is empty you can reduce these values by just manually
 * editing the database layout file.
 * If the table is not empty then these values can still be changed but they
 * must be changed by refactoring the database, hence, by invoking one of the
 * following three methods: {@link Refactor#nobsRowRef}, {@link
 * Refactor#nobsOutrowPtr}, and {@link Refactor#nobsRefCount}.
 * Note that if these values are reduced too much, the risk increases that the
 * corresponding maximum values described above are exceeded.
 * ACDP throws a {@link MaximumException MaximumException} if the maximum
 * values determined by these parameters are to be exceeded.
 *
 * @author Beat Hoermann
 */
public final class Setup {
	/**
	 * The {@code @Setup_Database} annotation is used by the Setup Tool to get
	 * the name of the database and the names of the database's tables.
	 * Optionally, values for the version of the database and for the class
	 * name and classpath of the cipher factory can be specified.
	 * <p>
	 * Note that the order of the table names in the "{@code tables}" array
	 * must be the same as the order in which the tables appear in the
	 * {@code open} method of the {@link CustomDatabase} class.
	 * It is the same order in which the table layouts appear in the resulting
	 * database layout.
	 * <p>
	 * This annotation is exclusively processed by the Setup Tool.
	 *
	 * @author Beat Hoermann
	 */
	@Retention(RetentionPolicy.RUNTIME)
	public @interface Setup_Database {
		/**
		 * The name of the database.
		 * 
		 * @return The name of the database.
		 */
		String name();
		/**
		 * The optional version of the database.
		 * 
		 * @return The version of the database.
		 */
		String version() default "";
		/**
		 * The optional name of the cipher factory class.
		 * 
		 * @return The name of the cipher factory class.
		 */
		String cipherFactoryClassName() default "";
		/**
		 * The optional classpath of the cipher factory.
		 * 
		 * @return The classpath of the cipher factory.
		 */
		String cipherFactoryClasspath() default "";
		/**
		 * The database's tables in proper order.
		 * 
		 * @return The database's tables in proper order.
		 */
		String[] tables();
	}
	
	/**
	 * The {@code @Setup_TableDeclaration} annotation is used by the Setup Tool
	 * to identify a table declaration within the database class.
	 * The table declaration is needed to find out the table class.
	 * <p>
	 * This annotation is exclusively processed by the Setup Tool.
	 *
	 * @author Beat Hoermann
	 */
	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.FIELD)
	public @interface Setup_TableDeclaration {
		/**
		 * The name of the table.
		 * The table name must also show up in the "{@link Setup_Database#tables
		 * tables}" array of the {@link Setup_Database} annotation.
		 * 
		 * @return The table name.
		 */
		String value();
	}
	
	/**
	 * The {@code @Setup_Table} annotation is used by the Setup Tool to get the
	 * names of the table's columns.
	 * <p>
	 * Note that the order of the column names in the "{@code value}" array
	 * must be the same as the order in which the columns appear in the
	 * constructor of the {@link CustomTable} class.
	 * It is the same order in which the column layouts appear in the table
	 * layout of the resulting database layout.
	 * <p>
	 * This annotation is exclusively processed by the Setup Tool.
	 *
	 * @author Beat Hoermann
	 */
	@Retention(RetentionPolicy.RUNTIME)
	public @interface Setup_Table {
		/**
		 * The table's columns in proper order.
		 * 
		 * @return The table's columns in proper order.
		 */
		String[] value();
	}
	
	/**
	 * The {@code @Setup_Column} annotation is used by the Setup Tool to
	 * identify the column <em>objects</em> declared within a table class.
	 * For this to work, each column must be declared {@code public static} and
	 * the table class itself must be declared with a {@code public} access level
	 * modifier.
	 * <p>
	 * Furthermore, for columns referencing a table, the name of the referenced
	 * table must be specified.
	 * <p>
	 * This annotation is exclusively processed by the Setup Tool.
	 *
	 * @author Beat Hoermann
	 */
	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.FIELD)
	public @interface Setup_Column {
		/**
		 * The name of the column.
		 * 
		 * @return The name of the column.
		 */
		String value();
		/**
		 * The name of the referenced table if the column is of {@link
		 * acdp.types.RefType RefType} or {@link acdp.types.ArrayOfRefType
		 * ArrayOfRefType}.
		 * 
		 * @return The name of the referenced table.
		 */
		String refdTable() default "";
	}
	
	/**
	 * Generates the database files from a database class, hence, from a class
	 * annotated with the {@link Setup_Database @Setup_Database} annotation and
	 * saves them to a given directory.
	 * <p>
	 * This method may throw an exception that is different from the listed
	 * exception.
	 * 
	 * @param  dbcn The name of the database class, for instance
	 *         "{@code com.example.MyDB}".
	 *         The database class is the class annotated with {@link
	 *         Setup_Database @Setup_Database}.
	 * @param  dir The directory where the generated database files are to be
	 *         saved.
	 *         If this value is {@code null} then this method takes as directory
	 *         the value returned by a call to {@code Paths.get(dbName)} where
	 *         {@code dbName} denotes the name of the database.
	 *        
	 * @throws IOFailureException If saving the genereated database files fails.
	 */
	public static final void run(String dbcn, Path dir) throws
																				IOFailureException {
		try {
			new SetupTool().run(dbcn, dir);
		} catch (FileIOException e) {
			throw new IOFailureException(e);
		}
	}
	
	/**
	 * The {@code main} method invoking the {@link #run} method.
	 * <p>
	 * The first element of the {@code args} array must contain the name of the
	 * database class, for instance "{@code com.example.MyDB}".
	 * If the {@code args} array contains a second element then this element is
	 * assumed to denote the directory where to save the generated database
	 * files.
	 * If the {@code args} array does not contain a second element then the
	 * generated database files are saved to the directory obtained by a call
	 * to {@code Paths.get(dbName)} where {@code dbName} denotes the name of
	 * the database.
	 * <p>
	 * This method may throw an exception that is different from the listed
	 * exceptions.
	 * 
	 * @param  args The command-line arguments.
	 *         The first element must be the name of the database class, for
	 *         instance "{@code com.example.MyDB}".
	 *         The database class is the class annotated with {@link
	 *         Setup_Database @Setup_Database}.
	 *         
	 * @throws IllegalArgumentException If {@code args} is empty or contains
	 *         more than two elements.
	 * @throws IOFailureException If saving the genereated database files fails.
	 */
	public static final void main(String[] args) throws IllegalArgumentException,
																				IOFailureException {
		if (args.length == 0 || args.length > 2)
			throw new IllegalArgumentException("The array of command-line " + 
							"arguments is empty or contains more than two arguments.");
		else {
			run(args[0], args.length > 1 ? Paths.get(args[1]) : null);
		}
	}
	
	/**
	 * Prevent object construction.
	 */
	private Setup() {
	}
}