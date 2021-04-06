
package com.cav.currencyexchange.service.s3;

import com.amazonaws.auth.AWSCredentialsProvider;


/**
 * Factory for creating S3 Wrappers.
 * @author Tony
 *
 */
public class S3WrapperFactory {
  /** Default implementation for IS3Wrapper */
  private Class<S3Wrapper> implementation = S3Wrapper.class;

  /**
   * Set the IS3Wrapper class.
   * 
   * @param clazz the IS3Wrapper class.
   */
  public static void setS3WrapperClass(final Class<S3Wrapper> clazz) {
    instance.implementation = clazz;
  }

  /** Singleton instance */
  private static final S3WrapperFactory instance = new S3WrapperFactory();

  /**
   * Creator method that creates a IS3Wrapper.
   * 
   * @param credentialsProvider the credentials provider.
   */
  public static S3Wrapper createS3Wrapper(final AWSCredentialsProvider credentialsProvider) {
    S3Wrapper retVal = null;
    try {
      retVal = (S3Wrapper) instance.implementation.newInstance();
      retVal.setCredentialsProvider(credentialsProvider);
    } catch (Exception e) {
      throw new RuntimeException("Problem Creating IS3Wrapper", e);
    }
    return retVal;
  }
}
