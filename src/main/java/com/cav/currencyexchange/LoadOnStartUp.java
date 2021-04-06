package com.cav.currencyexchange;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.util.Properties;
import java.util.concurrent.Executors;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.stereotype.Component;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.cav.currencyexchange.cache.AccountCache;
import com.cav.currencyexchange.service.s3.S3WrapperFactory;
import com.cav.currencyexchange.utils.Constants;

@Component
public class LoadOnStartUp {
	
	private static final Logger log = LoggerFactory.getLogger(LoadOnStartUp.class);
	
	protected static final String PARTNER_A_GBP = "PartnerAGBP";
	protected static final String PARTNER_A_USD = "PartnerAUSD";
	
	protected static final String PARTNER_B_GBP = "PartnerBGBP";
	protected static final String PARTNER_B_USD = "PartnerBUSD";
	
	protected static final String PARTNER_C_GBP = "PartnerCGBP";
	protected static final String PARTNER_C_USD = "PartnerCUSD";
	
	@PostConstruct
	private void init() {
		log.info("Load on start up  ");
		Properties prop = new Properties();
		 ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
			InputStream input = classLoader.getResourceAsStream("application.properties");
			if (input != null) {
				try {
					prop.load(input);
					
					//Apache MS
					Constants.ACTIVEMQ_BROKER = prop.getProperty("activemq.broker.url");
					log.info("ACTIVEMQ_BROKER "+Constants.ACTIVEMQ_BROKER);
					Constants.ACTIVEMQ_USER = prop.getProperty("activemq.activemq.user");
					log.info("ACTIVEMQ_USER "+Constants.ACTIVEMQ_USER);
					Constants.ACTIVEMQ_PASSWORD = prop.getProperty("activemq.activemq.password");
					log.info("ACTIVEMQ_PASSWORD "+Constants.ACTIVEMQ_PASSWORD);
					Constants.ACTIVEMQ_TOPIC = prop.getProperty("activemq.topic");
					log.info("ACTIVEMQ_TOPIC "+Constants.ACTIVEMQ_TOPIC);
					Constants.ACTIVEMQ_CLIENT_ID = prop.getProperty("activemq.clientid");
					log.info("ACTIVEMQ_CLIENT_ID "+Constants.ACTIVEMQ_CLIENT_ID);
					
					Constants.RECIEVED_ORDERS = prop.getProperty("recieved.orders");
					log.info("RECIEVED_ORDERS "+Constants.RECIEVED_ORDERS);
					Constants.MATCHED_ORDERS = prop.getProperty("matched.orders");
					log.info("MATCHED_ORDERS "+Constants.MATCHED_ORDERS);
					Constants.UNMATCHED_ORDERS = prop.getProperty("unmatched.orders");
					log.info("UNMATCHED_ORDERS "+Constants.UNMATCHED_ORDERS);
					
					Constants.S3_BUCKET = prop.getProperty("s3.bucket");
					log.info("S3_BUCKET "+Constants.S3_BUCKET);
					Constants.S3_ORDERS_PATH = prop.getProperty("s3.orders");
					log.info("S3_ORDERS_PATH "+Constants.S3_ORDERS_PATH);
					Constants.S3_MATCHED_PATH = prop.getProperty("s3.matched.orders");
					log.info("S3_MATCHED_PATH"+Constants.S3_MATCHED_PATH);
					Constants.S3_UNMATCHED_PATH = prop.getProperty("s3.unmatched.orders");
					log.info("S3_UNMATCHED_PATH "+Constants.S3_UNMATCHED_PATH);
					
					Constants.EXECUTOR = Executors.newFixedThreadPool(500);
					
					final AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();
					Constants.S3_WRAPPER = S3WrapperFactory.createS3Wrapper(credentialsProvider);

					addAccountFullMatch2Partners();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} finally {
					try {
						input.close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
	}
	
	 private  void addAccountFullMatch2Partners() {
			AccountCache.accounts.put(PARTNER_A_GBP, new BigDecimal(1000000));
			AccountCache.accounts.put(PARTNER_A_USD, new BigDecimal(1000000));
			AccountCache.accounts.put(PARTNER_B_GBP, new BigDecimal(1000000));
			AccountCache.accounts.put(PARTNER_B_USD, new BigDecimal(1000000));
		  }

}
