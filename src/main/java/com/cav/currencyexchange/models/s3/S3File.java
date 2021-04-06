/**
 * 
 */
package com.cav.currencyexchange.models.s3;

import java.time.LocalDateTime;

/**
 * 
 * @author Tony
 *
 */
public class S3File {
	private boolean directory = false;
	private boolean file = false;
	private String s3Bucket = null;
	private String absolutePath = null;
	private LocalDateTime lastModifiedDate = null;
	private long length = 0;

	/**
	 * Set the directory.
	 * 
	 * @param directory
	 *            the directory to set.
	 */
	public void setDirectory(boolean directory) {
		this.directory = directory;
	}

	/**
	 * Set the file.
	 * 
	 * @param file
	 *            the file to set.
	 */
	public void setFile(boolean file) {
		this.file = file;
	}

	/**
	 * Set the absolutePath.
	 * 
	 * @param absolutePath
	 *            the absolutePath to set.
	 */
	public void setAbsolutePath(final String absolutePath) {
		this.absolutePath = absolutePath;
	}

	/**
	 * Get the directory.
	 * 
	 * @return the directory.
	 */
	public boolean isDirectory() {
		return directory;
	}

	/**
	 * Get the file.
	 * 
	 * @return the file.
	 */
	public boolean isFile() {
		return file;
	}

	/**
	 * Get the absolutePath.
	 * 
	 * @return the absolutePath.
	 */
	public String getAbsolutePath() {
		return this.absolutePath;
	}

	/**
	 * Get the name of the file.
	 * 
	 * @return the name of the file.
	 */
	public String getName() {
		String name = null;
		if ((isDirectory()) && (this.absolutePath.endsWith("/"))) {
			name = this.absolutePath.substring(0,
					this.absolutePath.length() - 1);
		} else {
			name = this.absolutePath.substring(0);
		}
		return name;
	}

	/**
	 * Get the lastModifiedDate.
	 * 
	 * @return the lastModifiedDate.
	 */
	public LocalDateTime getLastModifiedDate() {
		return this.lastModifiedDate;
	}

	/**
	 * Set the lastModifiedDate.
	 * 
	 * @param lastModifiedDate
	 *            the lastModifiedDate to set.
	 */
	public void setLastModifiedDate(LocalDateTime lastModifiedDate) {
		this.lastModifiedDate = lastModifiedDate;
	}

	/**
	 * Get the length.
	 * 
	 * @return the length.
	 */
	public long getLength() {
		return this.length;
	}

	/**
	 * Set the length.
	 *
	 * @param length
	 *            the length to set.
	 */
	public void setLength(final long length) {
		this.length = length;
	}

	/**
	 * Get the s3Bucket.
	 * 
	 * @return the s3Bucket.
	 */
	public String getS3Bucket() {
		return this.s3Bucket;
	}

	/**
	 * Set the s3Bucket.
	 * 
	 * @param s3Bucket
	 *            the s3Bucket to set.
	 */
	public void setS3Bucket(final String s3Bucket) {
		this.s3Bucket = s3Bucket;
	}

	/**
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		final StringBuffer retVal = new StringBuffer();
		retVal.append(this.absolutePath);
		if (isFile()) {
			retVal.append(", File, Length ").append(this.length).append(", ")
					.append(this.lastModifiedDate.toString());
		} else {
			retVal.append(", Directory");
		}
		return retVal.toString();
	}

	/**
	 * Get an S3 key from the prefix and file name.
	 * 
	 * @param prefix
	 *            the prefix.
	 * @param fileName
	 *            the file name.
	 * @return the S3 key.
	 */
	public static String createS3Key(final String prefix,
			final String fileName) {
		if (prefix.endsWith("/")) {
			return prefix + fileName;
		}
		return prefix + "/" + fileName;
	}
}
