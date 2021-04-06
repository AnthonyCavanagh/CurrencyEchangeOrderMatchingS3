/*
 * Wraps the Amazon S3 service.
 * 
 * S3Wrapper.java
 * 
 * Created 9/3/2017
 * 
 * (c) EnAppSys Ltd
 */
package com.cav.currencyexchange.service.s3;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.cav.currencyexchange.models.s3.S3File;



/**
 * Wraps the Amazon S3 service.
 * @author Tony
 *
 */
public class S3WrapperImpl implements S3Wrapper {
  private AWSCredentialsProvider credentialsProvider = null;
  private AmazonS3 s3 = null;

  protected S3WrapperImpl() {
    //StaticInformation.init();
  }

  /**
   * @see com.enappsys.aws.wrapper.s3.IS3Wrapper#setCredentialsProvider(AWSCredentialsProvider)
   */
  @Override
  public void setCredentialsProvider(final AWSCredentialsProvider credentialsProvider) {
    this.credentialsProvider = credentialsProvider;
  }

  private AmazonS3 getS3Client() {
    if (null == this.s3) {
      this.s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.EU_WEST_1).withCredentials(this.credentialsProvider)
          .build();
    }
    return this.s3;
  }

  /**
   * @see com.enappsys.aws.wrapper.s3.IS3Wrapper#listFiles(java.lang.String, java.lang.String)
   */
  @Override
  public S3File[] listFiles(final String s3Bucket, final String s3Key) {
    String pathToUse = s3Key.substring(0);
    if (pathToUse.startsWith("/")) {
      pathToUse = pathToUse.substring(1);
    }
    if ((pathToUse.length() > 0) && (!pathToUse.endsWith("/"))) {
      pathToUse = pathToUse + "/";
    }

    final Vector<S3File> contents = new Vector<S3File>();

    final List<String> subDirs = getS3SubDirectories(getS3Client(), s3Bucket, pathToUse);
    for (String subDir : subDirs) {
      contents.add(createDirectory(s3Bucket, subDir));
    }

    final ListObjectsRequest lor = new ListObjectsRequest().withBucketName(s3Bucket).withPrefix(pathToUse);
    ObjectListing objectListing = null;

    do {
      objectListing = getS3Client().listObjects(lor);
      final List<S3ObjectSummary> summaries = objectListing.getObjectSummaries();
      for (S3ObjectSummary summary : summaries) {
        final String key = summary.getKey();
        if ((key.length() > pathToUse.length()) && (!key.substring(pathToUse.length() + 1).contains("/"))) {
          contents.add(createFile(summary));
        }
      }
      lor.setMarker(objectListing.getNextMarker());
    } while (objectListing.isTruncated());
    return contents.toArray(new S3File[contents.size()]);
  }

  /**
   * @see com.enappsys.aws.wrapper.s3.IS3Wrapper#listFiles(java.lang.String, java.lang.String, int)
   */
  @Override
  public S3File[] listFiles(final String s3Bucket, final String s3Key, final int maxItems) {
    String pathToUse = s3Key.substring(0);
    if (pathToUse.startsWith("/")) {
      pathToUse = pathToUse.substring(1);
    }
    if ((pathToUse.length() > 0) && (!pathToUse.endsWith("/"))) {
      pathToUse = pathToUse + "/";
    }

    final Vector<S3File> contents = new Vector<S3File>();

    // final List<String> subDirs = getS3SubDirectories(getS3Client(), s3Bucket,
    // pathToUse);
    // for (String subDir : subDirs) {
    // contents.add(createDirectory(s3Bucket, subDir));
    // }

    final ListObjectsRequest lor =
        new ListObjectsRequest().withBucketName(s3Bucket).withPrefix(pathToUse).withMaxKeys(new Integer(100));
    ObjectListing objectListing = null;
    int totalSize = 0;
    do {
      objectListing = getS3Client().listObjects(lor);
      final List<S3ObjectSummary> summaries = objectListing.getObjectSummaries();
      int size = summaries.size();
      totalSize += size;
      for (S3ObjectSummary summary : summaries) {
        final String key = summary.getKey();
        if ((key.length() > pathToUse.length()) && (!key.substring(pathToUse.length() + 1).contains("/"))) {
          contents.add(createFile(summary));
        }
      }
      lor.setMarker(objectListing.getNextMarker());
    } while ((objectListing.isTruncated()) && (totalSize <= maxItems));
    return contents.toArray(new S3File[contents.size()]);
  }

  /**
   * @see com.enappsys.aws.wrapper.s3.IS3Wrapper#getFile(java.lang.String, java.lang.String)
   */
  @Override
  public S3File getFile(final String s3bucket, final String s3key) {
    final S3File retVal = new S3File();
    retVal.setS3Bucket(s3bucket);
    retVal.setAbsolutePath(s3key);
    try {
      final ObjectMetadata objectMetadata = this.getS3Client().getObjectMetadata(s3bucket, s3key);
      retVal.setLastModifiedDate(LocalDateTime.now());
      retVal.setLength(objectMetadata.getContentLength());
    } catch (Exception ignore) {
    }
    return retVal;
  }

  /**
   * @see com.enappsys.aws.wrapper.s3.IS3Wrapper#copyFileToS3fromWeb(java.lang.String, java.lang.String,
   *      java.lang.String)
   */
  @Override
  public void copyFileToS3fromWeb(final String s3bucket, final String s3key, final String uri) throws Exception {

    HttpURLConnection urlConn = null;
    InputStream is = null;
    try {
      // Send data
      final URL url = new URL(uri);
      urlConn = (HttpURLConnection) url.openConnection();
      urlConn.setDoOutput(true);
      urlConn.setRequestMethod("GET");

      // Get the response
      is = urlConn.getInputStream();
      this.copyStreamToS3(s3bucket, s3key, is);
    } catch (Exception e) {
      throw e;
    } finally {
      try {
        if (null != is) {
          is.close();
        }
      } catch (Exception ignore) {
        ignore.printStackTrace();
      }
      try {
        urlConn.disconnect();
      } catch (Exception ignore) {
        ignore.printStackTrace();
      }
    }
  }

  /**
   * Get the last modified date/time.
   * 
   * @param s3object the S3 Object.
   * @return the last modified date/time.
   */
  private LocalDateTime getLastModified(final S3ObjectSummary s3object) {
    final Date lastModifiedDate = s3object.getLastModified();
    return LocalDateTime.ofInstant(lastModifiedDate.toInstant(),ZoneId.systemDefault());
  }


  private S3File createDirectory(final String s3Bucket, final String absolutePath) {
    final S3File retVal = new S3File();
    retVal.setDirectory(true);
    retVal.setFile(false);
    retVal.setS3Bucket(s3Bucket);
    retVal.setAbsolutePath(absolutePath);
    return retVal;
  }

  private S3File createFile(final S3ObjectSummary s3object) {
    final String s3Bucket = s3object.getBucketName();
    final String absolutePath = s3object.getKey();
    final LocalDateTime lastModifiedDate = getLastModified(s3object);
    final long length = s3object.getSize();

    final S3File retVal = new S3File();
    retVal.setDirectory(false);
    retVal.setFile(true);
    retVal.setS3Bucket(s3Bucket);
    retVal.setAbsolutePath(absolutePath);
    retVal.setLastModifiedDate(lastModifiedDate);
    retVal.setLength(length);
    return retVal;
  }



  /**
   * @see com.enappsys.aws.wrapper.s3.IS3Wrapper#copyFileInS3(java.lang.String, java.lang.String, java.lang.String)
   */
  @Override
  public void copyFileInS3(final String s3Bucket, final String fromS3Key, final String toS3Key) {
    copyFileAcrossS3(s3Bucket, fromS3Key, s3Bucket, toS3Key);
  }

  /**
   * @see com.enappsys.aws.wrapper.s3.IS3Wrapper#copyFileAcrossS3(java.lang.String, java.lang.String, java.lang.String,
   *      java.lang.String)
   */
  @Override
  public void copyFileAcrossS3(final String fromS3Bucket, final String fromS3Key, final String toS3Bucket,
      final String toS3Key) {
   
    getS3Client().copyObject(fromS3Bucket, fromS3Key, toS3Bucket, toS3Key);
  }

  private List<String> getS3SubDirectories(final AmazonS3 s3Client, final String bucketName, final String keyPrefix) {
    String keyPrefixToUse = keyPrefix.substring(0);
    List<String> paths = new ArrayList<String>();
    String delimiter = "/";
    if (keyPrefixToUse != null && !keyPrefixToUse.isEmpty() && !keyPrefixToUse.endsWith(delimiter)) {
      keyPrefixToUse += delimiter;
    }

    ListObjectsRequest listObjectRequest =
        new ListObjectsRequest().withBucketName(bucketName).withPrefix(keyPrefixToUse).withDelimiter(delimiter);

    ObjectListing objectListing;
    do {
      objectListing = s3Client.listObjects(listObjectRequest);
      paths.addAll(objectListing.getCommonPrefixes());
      listObjectRequest.setMarker(objectListing.getNextMarker());
    } while (objectListing.isTruncated());
    return paths;
  }

  /**
   * @see com.enappsys.aws.wrapper.s3.IS3Wrapper#writeFileToS3(java.lang.String, java.lang.String, InputStream,
   *      ZonedDateTime, long)
   */
  @Override
  public void writeFileToS3(final String s3bucket, final String key, final InputStream inputStream,
      final ZonedDateTime lastModifiedDateTime, final long contentLength) {
    final ObjectMetadata metadata = new ObjectMetadata();
    metadata.setLastModified(Date.from(lastModifiedDateTime.toInstant()));
    metadata.setContentLength(contentLength);
    final PutObjectRequest putObjectRequest = new PutObjectRequest(s3bucket, key, inputStream, metadata);
    getS3Client().putObject(putObjectRequest);
  }

  /**
   * @see com.enappsys.aws.wrapper.s3.IS3Wrapper#moveFileToS3(java.lang.String, java.lang.String, java.lang.String)
   */
  @Override
  public void moveFileToS3(final String s3bucket, final String key, final String filePath) {
    copyFileToS3(s3bucket, key, filePath);
    new File(filePath).delete();
  }

  /**
   * @see com.enappsys.aws.wrapper.s3.IS3Wrapper#moveFileInS3(java.lang.String, java.lang.String, java.lang.String)
   */
  @Override
  public void moveFileInS3(final String s3Bucket, final String fromS3Key, final String toS3Key) {
    if (!(fromS3Key.equals(toS3Key))) {
      copyFileInS3(s3Bucket, fromS3Key, toS3Key);
      deleteFileFromS3(s3Bucket, fromS3Key);
    }
  }


  /**
   * @see com.enappsys.aws.wrapper.s3.IS3Wrapper#moveFileAcrossS3(java.lang.String, java.lang.String, java.lang.String,
   *      java.lang.String)
   */
  @Override
  public void moveFileAcrossS3(final String fromS3Bucket, final String fromS3Key, final String toS3Bucket,
      final String toS3Key) {
    copyFileAcrossS3(fromS3Bucket, fromS3Key, toS3Bucket, toS3Key);
    deleteFileFromS3(fromS3Bucket, fromS3Key);
  }

  /**
   * @see com.enappsys.aws.wrapper.s3.IS3Wrapper#copyFileToS3(java.lang.String, java.lang.String, java.lang.String)
   */
  @Override
  public void copyFileToS3(final String s3bucket, final String key, final String filePath) {
    final File file = new File(filePath);
    final PutObjectRequest putObjectRequest = new PutObjectRequest(s3bucket, key, file);
    getS3Client().putObject(putObjectRequest);
  }

  /**
   * @see com.enappsys.aws.wrapper.s3.IS3Wrapper#copyStringToS3(java.lang.String, java.lang.String, java.lang.String)
   */
  @Override
  public void copyStringToS3(final String s3bucket, final String key, final String string) {
    final byte[] contentAsBytes = string.getBytes();
    copyBytesToS3(s3bucket, key, contentAsBytes);
  }

  /**
   * @see com.enappsys.aws.wrapper.s3.IS3Wrapper#copyBytesToS3(java.lang.String, java.lang.String, byte[])
   */
  @Override
  public void copyBytesToS3(final String s3bucket, final String key, final byte[] bytes) {
    final ByteArrayInputStream contentsAsStream = new ByteArrayInputStream(bytes);
    final ObjectMetadata md = new ObjectMetadata();
    md.setContentLength(bytes.length);
    getS3Client().putObject(new PutObjectRequest(s3bucket, key, contentsAsStream, md));
  }

  /**
   * @see com.enappsys.aws.wrapper.s3.IS3Wrapper#copyStreamToS3(java.lang.String, java.lang.String, java.io.InputStream,
   *      long)
   */
  @Override
  public void copyStreamToS3(final String s3bucket, final String key, final InputStream is, final long contentLength) {
    final ObjectMetadata metaData = new ObjectMetadata();
    metaData.setContentLength(contentLength);
    final PutObjectRequest putObjectRequest = new PutObjectRequest(s3bucket, key, is, metaData);
    getS3Client().putObject(putObjectRequest);
  }

  /**
   * @see com.enappsys.aws.wrapper.s3.IS3Wrapper#copyStreamToS3(java.lang.String, java.lang.String, java.io.InputStream)
   */
  @Override
  public void copyStreamToS3(final String s3bucket, final String key, final InputStream is) {
    final ObjectMetadata metaData = new ObjectMetadata();
    final PutObjectRequest putObjectRequest = new PutObjectRequest(s3bucket, key, is, metaData);
    getS3Client().putObject(putObjectRequest);
  }

  /**
   * @see com.enappsys.aws.wrapper.s3.IS3Wrapper#copyFileFromS3(java.lang.String, java.lang.String, java.lang.String)
   */
  @Override
  public void copyFileFromS3(final String s3bucket, final String key, final String filePath) {
    final S3Object s3Object = getS3Client().getObject(s3bucket, key);
    InputStream in = null;
    OutputStream out = null;
    try {
      in = s3Object.getObjectContent();
      out = new FileOutputStream(filePath);
      byte[] buf = new byte[1024];
      int count = 0;
      while ((count = in.read(buf)) != -1) {
        if (Thread.interrupted()) {
          throw new InterruptedException();
        }
        out.write(buf, 0, count);
      }
    } catch (Exception e) {
     e.printStackTrace();
    } finally {
      try {
        out.close();
      } catch (Exception ignore) {
      }
      try {
        in.close();
      } catch (Exception ignore) {
      }
    }
  }

  /**
   * @see com.enappsys.aws.wrapper.s3.IS3Wrapper#deleteFileFromS3(java.lang.String, java.lang.String)
   */
  @Override
  public void deleteFileFromS3(final String s3bucket, final String s3key) {

    getS3Client().deleteObject(s3bucket, s3key);
  }

  /**
   * @see com.enappsys.aws.wrapper.s3.IS3Wrapper#getInputStream(java.lang.String, java.lang.String)
   */
  @Override
  public InputStream getInputStream(final String s3bucket, final String s3key) {
    S3Object s3Object = null;
    try {
      s3Object = getS3Client().getObject(s3bucket, s3key);
    } catch (SdkClientException e) {
      throw new RuntimeException("Problem getting S3 Object at '" + s3bucket + " - " + s3key, e);
    }
    final S3ObjectInputStream s3ObjectContent = s3Object.getObjectContent();
    return s3ObjectContent;
  }

 
  @Override
  public ObjectMetadata getMetaData(final String s3bucket, final String s3key) {
	  S3Object s3Object = getS3Client().getObject(s3bucket, s3key);
	  ObjectMetadata metaData = s3Object.getObjectMetadata();
	return metaData;
  }


  /**
   * @see com.enappsys.aws.wrapper.s3.IS3Wrapper#doesFileExist(java.lang.String, java.lang.String)
   */
  @Override
  public boolean doesFileExist(String s3bucket, String key) {
    return getS3Client().doesObjectExist(s3bucket, key);
  }

  /**
   * @see com.enappsys.aws.wrapper.s3.IS3Wrapper#getDirectorySize(java.lang.String, java.lang.String)
   */
  @Override
  public int getDirectorySize(String s3bucket, String s3prefix) {
    int size = 0;
    final ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(s3bucket).withPrefix(s3prefix);
    ListObjectsV2Result result;
    do {
      result = getS3Client().listObjectsV2(req);
      size += result.getObjectSummaries().size();
      req.setContinuationToken(result.getNextContinuationToken());
    } while (result.isTruncated());
    return size;
  }

@Override
public String logSizeOfDirectory(AmazonCloudWatch cloudwatch, String bucket, String s3prefix) {
	// TODO Auto-generated method stub
	return null;
}


@Override
public void moveFileToS3(final String s3bucket, final String key, final String filePath, String publishdate) {
	copyFileToS3(s3bucket, key, filePath, publishdate);
	new File(filePath).delete();
}

@Override
public void moveFileToS3Settlement(final String s3bucket, final String key, final String filePath, String publishdate) {
	copyFileToS3(s3bucket, key, filePath, publishdate);
}


private void copyFileToS3(final String s3bucket, final String key, final String filePath, String publishdate) {
	AmazonS3 client = getS3Client();
    final File file = new File(filePath);
    ObjectMetadata metadata = new ObjectMetadata();
    metadata.setContentType("plain/text");
    Map<String, String> userMetadata = new  HashMap<String, String>();
    userMetadata.put("LastPublishedTime", publishdate);
	metadata.setUserMetadata(userMetadata );
    final PutObjectRequest putObjectRequest = new PutObjectRequest(s3bucket, key, file);
    putObjectRequest.setMetadata(metadata);
    client.putObject(putObjectRequest);
 }






}
