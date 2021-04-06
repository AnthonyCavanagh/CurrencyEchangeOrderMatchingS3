package com.cav.currencyexchange.service.s3.write;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cav.currencyexchange.models.CurrencyOrder;
import com.cav.currencyexchange.utils.Constants;
import com.cav.currencyexchangebroker.generated.Orders.Order;

public class S3WriteDateServiceImpl implements S3WriteDateService {
	
	private static final String TDIR = System.getProperty("java.io.tmpdir");
	private static final Logger log = LoggerFactory.getLogger(S3WriteDateServiceImpl.class);
	
	DateTimeFormatter formatter1 = DateTimeFormatter.ofPattern("yyyy-MM-dd:HH:mm:ss");
	DateTimeFormatter formatter2 = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
	DateTimeFormatter formatter3 = DateTimeFormatter.ofPattern("yyyyMMdd");
	
	String s3Path = null;
	List <Order> messages = null;
	List <CurrencyOrder> orders = null;
	String recieved = null;
	String fileTime = null;
	String fileDate = null;
	String currencyKeyPair = null;
	String orderType = null;

	public S3WriteDateServiceImpl(String s3Path, LocalDateTime recievedTime, List<Order> messages) {
		super();
		this.s3Path = s3Path;
		this.recieved = recievedTime.format(formatter1);
		this.fileTime = recievedTime.format(formatter2);
		this.messages = messages;
	}

	public S3WriteDateServiceImpl(String s3Path, List<CurrencyOrder> orders, LocalDateTime recievedTime,
			String currencyKeyPair, String orderType) {
		super();
		this.s3Path = s3Path;
		this.orders = orders;
		this.fileTime = recievedTime.format(formatter2);
		this.fileDate = recievedTime.format(formatter3);
		this.currencyKeyPair = currencyKeyPair;
		this.orderType = orderType;
	}

	@Override
	public Object call() throws Exception {
		if(messages != null) {
			writeMessagesToS3(s3Path, Constants.S3_FILE_NAME+recieved, messages);
		} else {
			String[] fields = this.currencyKeyPair.split("/");
			StringBuilder sb = new StringBuilder(s3Path)
					.append(this.orderType)
					.append("/")
					.append(fields[0])
					.append(fields[1])
					.append("/").append(this.fileDate).append("/");
			
			writeOrdersToS3(sb.toString(), Constants.S3_FILE_NAME+recieved, orders);
		}
		return null;
	}
	
	public void writeMessagesToS3(String s3Path, String s3File, List <Order> orders) {
		String filePath = TDIR +"/"+s3File;
		writeTempMessagesFileGZipped(filePath, orders);
		Constants.S3_WRAPPER.moveFileToS3(Constants.S3_BUCKET, s3Path, filePath);
	}
	
	public void writeOrdersToS3(String s3Path, String s3File, List <CurrencyOrder> orders) {
		String filePath = TDIR +"/"+s3File;
		writeTempOrdersFileGZipped(filePath, orders);
		Constants.S3_WRAPPER.moveFileToS3(Constants.S3_BUCKET, s3Path, filePath);
	}
	
	private String writeTempMessagesFileGZipped(String key, List <Order> orders) {
		GZIPOutputStream gzipOS = null;
		ByteArrayOutputStream writer = null;
		String publishDateTime = null;
		try {
			gzipOS = new GZIPOutputStream(new FileOutputStream(key));
			writer = new ByteArrayOutputStream();
			for(Order order : orders) {
				writer.write(order.toString().getBytes());
				writer.write("\n".getBytes());
			}
			byte[] messageBytes = writer.toByteArray();
			gzipOS.write(messageBytes, 0,messageBytes.length);
		} catch (IOException e) {
			log.error("writeTempFileGZipped No writing for file "+key);
		} finally {
			if(writer != null) {
				try {
					writer.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if(gzipOS != null) {
				try {
					gzipOS.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		return publishDateTime;
	}
	
	private String writeTempOrdersFileGZipped(String key, List <CurrencyOrder> orders) {
		GZIPOutputStream gzipOS = null;
		ByteArrayOutputStream writer = null;
		String publishDateTime = null;
		try {
			gzipOS = new GZIPOutputStream(new FileOutputStream(key));
			writer = new ByteArrayOutputStream();
			for(CurrencyOrder order : orders) {
				writer.write(order.toString().getBytes());
				writer.write("\n".getBytes());
			}
			byte[] messageBytes = writer.toByteArray();
			gzipOS.write(messageBytes, 0,messageBytes.length);
		} catch (IOException e) {
			log.error("writeTempFileGZipped No writing for file "+key);
		} finally {
			if(writer != null) {
				try {
					writer.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if(gzipOS != null) {
				try {
					gzipOS.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		return publishDateTime;
	}

}
