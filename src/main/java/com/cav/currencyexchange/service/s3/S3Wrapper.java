/*
 * Wraps the Amazon S3 service.
 * 
 * S3Wrapper.java
 * 
 * Created 7/2/2017
 * 
 * (c) EnAppSys Ltd
 */
package com.cav.currencyexchange.service.s3;

import java.io.InputStream;
import java.time.ZonedDateTime;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.cav.currencyexchange.models.s3.S3File;


/**
 * 
 * @author Tony
 *
 */
public interface S3Wrapper {
  /**
   * Set the credentials provider.
   * 
   * @param credentialsProvider the credentials provider.
   */
  public void setCredentialsProvider(final AWSCredentialsProvider credentialsProvider);

  /**
   * List the files in a directory.
   * 
   * @param bucket the S3 bucket.
   * @param path the path.
   * @return the objects in the directory.
   */
  public S3File[] listFiles(final String bucket, final String path);

  /**
   * List the files in a directory.
   * 
   * @param bucket the S3 bucket.
   * @param path the path.
   * @param maxItems the maximum number of items to return.
   * @return the objects in the directory.
   */
  public S3File[] listFiles(final String bucket, final String path, final int maxItems);

  /**
   * Get the file detail.
   * 
   * @param s3bucket the S3 bucket.
   * @param s3key the S3 key.
   * @return the file detail.
   */
  public S3File getFile(final String s3bucket, final String s3key);

  /**
   * Write to S3 using an input stream.
   * 
   * @param bucket the S3 bucket.
   * @param key the object key
   * @param inputStream the input stream.
   * @param lastModifiedDateTime the last modified date time of the file.
   * @param contentLength the context length.
   */
  public void writeFileToS3(final String s3bucket, final String key, final InputStream inputStream,
      final ZonedDateTime lastModifiedDateTime, final long contentLength);

  /**
   * Write to S3 using a file.
   * 
   * @param bucket the S3 bucket.
   * @param key the object key
   * @param filePath the file path.
   */
  public void copyFileToS3(final String s3bucket, final String key, final String filePath);

  /**
   * Write to S3 using a file.
   * 
   * @param bucket the S3 bucket.
   * @param key the object key
   * @param filePath the file path.
   */
  public void moveFileToS3(final String s3bucket, final String key, final String filePath);

  /**
   * Write to S3 using from a web URI.
   * 
   * @param bucket the S3 bucket.
   * @param key the object key
   * @param uri the web URI.
   * @throws Exception the exception.
   */
  public void copyFileToS3fromWeb(final String s3bucket, final String key, final String uri) throws Exception;

  /**
   * Write to S3 using an input stream.
   * 
   * @param s3bucket the S3 bucket.
   * @param key the object key
   * @param is the input stream.
   * @param contentLength TODO
   */
  public void copyStreamToS3(final String s3bucket, final String key, final InputStream is, final long contentLength);

  /**
   * Write to S3 using an input stream.
   * 
   * @param s3bucket the S3 bucket.
   * @param key the object key
   * @param string the string to copy.
   */
  public void copyStringToS3(final String s3bucket, final String key, final String string);

  /**
   * Write to S3 using an input stream.
   * 
   * @param s3bucket the S3 bucket.
   * @param key the object key
   * @param bytes the bytes to copy.
   */
  public void copyBytesToS3(final String s3bucket, final String key, final byte[] bytes);

  /**
   * Write to S3 using an input stream.
   * 
   * @param s3bucket the S3 bucket.
   * @param key the object key
   * @param is the input stream.
   */
  public void copyStreamToS3(final String s3bucket, final String key, final InputStream is);

  /**
   * Get from S3 and put in a directory as a file.
   * 
   * @param s3bucket the S3 bucket.
   * @param s3Key the object key.
   * @param filePath the file path.
   */
  public void copyFileFromS3(final String s3bucket, final String s3Key, final String filePath);

  /**
   * Copy a file in a S3 bucket.
   * 
   * @param s3Bucket the S3 bucket.
   * @param fromS3Key the from object key.
   * @param toS3Key the to object key.
   */
  public void copyFileInS3(final String s3Bucket, final String fromS3Key, final String toS3Key);

  /**
   * Copy a file across S3 buckets.
   * 
   * @param fromS3Bucket the from S3 bucket.
   * @param fromS3Key the from object key.
   * @param toS3Bucket the to S3 bucket.
   * @param toS3Key the to object key.
   */
  public void copyFileAcrossS3(final String fromS3Bucket, final String fromS3Key, final String toS3Bucket,
      final String toS3Key);

  /**
   * Move a file in a S3 bucket.
   * 
   * @param s3Bucket the S3 bucket.
   * @param fromS3Key the from object key.
   * @param toS3Key the to object key.
   */
  public void moveFileInS3(final String s3Bucket, final String fromS3Key, final String toS3Key);

  /**
   * Move a file across S3 buckets.
   * 
   * @param fromS3Bucket the from S3 bucket.
   * @param fromS3Key the from object key.
   * @param toS3Bucket the to S3 bucket.
   * @param toS3Key the to object key.
   */
  public void moveFileAcrossS3(final String fromS3Bucket, final String fromS3Key, final String toS3Bucket,
      final String toS3Key);

  /**
   * Delete object from S3.
   * 
   * @param s3Bucket the S3 bucket.
   * @param s3Key the S3 object key
   */
  public void deleteFileFromS3(final String s3Bucket, final String s3Key);

  /**
   * Get Input stream from S3 Object.
   * 
   * @param bucket the S3 bucket.
   * @param key the object key
   */
  public InputStream getInputStream(final String s3bucket, final String key);

  /**
   * Log the size of the directory to cloudwatch.
   * 
   * @param cloudwatch the cloudwatch client.
   * @param bucket the S3 bucket.
   * @param path the path.
   * @return feedback
   */
  public String logSizeOfDirectory(final AmazonCloudWatch cloudwatch, final String bucket, final String s3prefix);



  /**
   * Check if the file exists in S3
   * 
   * @param s3bucket the s3 bucket
   * @param key the object key
   * @return exists
   */
  public boolean doesFileExist(final String s3bucket, final String key);

  /**
   * Get the directory size (including sub-dirs)
   * 
   * @param s3bucket
   * @param s3prefix
   * @return the directory size
   */
  public int getDirectorySize(final String s3bucket, final String s3prefix);

ObjectMetadata getMetaData(String s3bucket, String s3key);

void moveFileToS3(String s3bucket, String key, String filePath, String publishdate);

void moveFileToS3Settlement(String s3bucket, String key, String filePath, String publishdate);

}
