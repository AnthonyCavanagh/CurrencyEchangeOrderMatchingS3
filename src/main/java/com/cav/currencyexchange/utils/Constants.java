package com.cav.currencyexchange.utils;

import java.util.concurrent.ExecutorService;

import com.cav.currencyexchange.service.s3.S3Wrapper;

public class Constants {

	public static String ACTIVEMQ_BROKER = null;
	public static String ACTIVEMQ_USER = null;
	public static String ACTIVEMQ_PASSWORD = null;
	public static String ACTIVEMQ_TOPIC = null;
	public static String ACTIVEMQ_CLIENT_ID = null;
	
	public static String RECIEVED_ORDERS = null;
	public static String MATCHED_ORDERS = null;
	public static String UNMATCHED_ORDERS = null;
	public static String REMOVE_ORDERS = null;
	public static ExecutorService EXECUTOR = null;
	
	public static S3Wrapper S3_WRAPPER = null;
	public static String S3_BUCKET = null;
	public static String S3_ORDERS_PATH = null;
	public static String S3_MATCHED_PATH = null;
	public static String S3_UNMATCHED_PATH = null;
	
	public static String S3_FILE_NAME = null;
	
	
	
}
