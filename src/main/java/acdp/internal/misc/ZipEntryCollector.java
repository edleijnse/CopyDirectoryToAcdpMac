/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.internal.misc;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import acdp.misc.Layout;

/**
 * Provides methods for adding a file to a zip archive.
 * 
 * @author Beat Hoermann
 */
public final class ZipEntryCollector {
	/**
	 * The directory against which the relative file path of a layout entry is
	 * resolved.
	 */
	private final Path dir;
	/**
	 * The root directory within the zip archive or {@code null} if the files
	 * have no common root directory.
	 */
	private final Path rootDir;
	/**
	 * The output stream of the zip archive.
	 */
	private final ZipOutputStream zos;
	
	/**
	 * The constructor.
	 * 
	 * @param dir The directory against which the relative file paths of a
	 *        layout entry are resolved.
	 *        This value is only used by the {@link #add(String, Layout)} and
	 *        the {@link #add(String, Layout, Supplier)} methods.
	 * @param rootDir The root directory or {@code null} if the files in the zip
	 *        archive should not be placed in a root directory.
	 * @param zos The output stream of the zip archive.
	 */
	public ZipEntryCollector(Path dir, Path rootDir, ZipOutputStream zos) {
		this.dir = dir;
		this.rootDir = rootDir;
		this.zos = zos;
	}
	
	/**
	 * Creates a new zip entry for the specified file.
	 * Uses the forward slash ('/') for separating path elements.
	 * 
	 * @param  file The file.
	 * 
	 * @return The created zip entry.
	 */
	private final ZipEntry createZipEntry(Path file) {
		// Invoke file.normalize in order to eliminate "." and ".." path
		// elements because unzip cannot cope with ".." path element.
		final Path path = file.normalize();
		return new ZipEntry((rootDir == null ? path : rootDir.resolve(path)).
												toString().replace(File.separator,  "/"));
	}
	
	/**
	 * A supplier writes data from a file to an output stream.
	 * <p>
	 * A supplier is used when {@link Files#copy(Path, OutputStream)} can't be
	 * used because the file is locked.
	 * 
	 * @author Beat Hoermann
	 */
	@FunctionalInterface
	public interface Supplier {
		/**
		 * Writes data from a file to the specified output stream.
		 * <p>
		 * The specified output stream is not closed.
		 * 
		 * @param  os The output stream.
		 * 
		 * @throws IOException If an I/O error occurs.
		 */
		public void run(OutputStream os) throws IOException;
	}
	
	/**
	 * Adds the specified file to the zip archive.
	 * <p>
	 * The file appears in the zip archive without any path information, just
	 * with its file name.
	 * For example, if the specified file path is "a/b/c" (or "a\b\c") then
	 * unzipping the zip archive will just produce file "c".
	 * 
	 * @param  file The file to be added to the zip archive.
	 * @param  supplier The supplier delivering the content of the file.
	 * 
	 * @throws IOException If an I/O error occurs.
	 */
	public final void add(Path file, Supplier supplier) throws IOException {
		zos.putNextEntry(createZipEntry(file.getFileName()));
		supplier.run(zos);
		zos.closeEntry();
	}
	
	/**
	 * This method works in the same way as the {@link #add(Path, Supplier)}
	 * method except that it uses its own internal supplier.
	 * <p>
	 * This method works only if the file is not locked.
	 * 
	 * @param  file The file to be added to the zip archive.
	 * 
	 * @throws IOException If an I/O error occurs, including the case where the
	 *         file is locked.
	 */
	public final void add(Path file) throws IOException {
		add(file, os -> Files.copy(file, os));
	}
	
	/**
	 * Adds the file denoted by the file path saved in the specified layout
	 * entry to the zip archive.
	 * <p>
	 * If the file name is preceded by a path then the path is retained.
	 * For example, if the layout entry is "a/b/c" (or "a\b\c") then unzipping
	 * the zip archive will produce "a/b/c".
	 * 
	 * @param  key The key.
	 * @param  layout The layout.
	 *         The layout must contain a valid file path string for the
	 *         specified key.
	 * @param  supplier The supplier delivering the content of the file.
	 * 
	 * @throws IOException If an I/O error occurs.
	 */
	public final void add(String key, Layout layout, Supplier supplier) throws
																						IOException {
		final Path file = Paths.get(layout.getString(key));
		zos.putNextEntry(createZipEntry(file));
		supplier.run(zos);
		zos.closeEntry();
	}
	
	/**
	 * This method works in the same way as the {@link #add(String, Layout,
	 * Supplier)} method except that it uses its own internal supplier.
	 * <p>
	 * This method works only if the file is not locked.
	 * 
	 * @param  key The key.
	 * @param  layout The layout.
	 *         The layout must contain a valid file path string for the
	 *         specified key.
	 * 
	 * @throws IOException If an I/O error occurs, including the case where the
	 *         file is locked.
	 */
	public final void add(String key, Layout layout) throws IOException {
		final Path file = Paths.get(layout.getString(key));
		zos.putNextEntry(createZipEntry(file));
		Files.copy(dir.resolve(file), zos);
		zos.closeEntry();
	}
}
