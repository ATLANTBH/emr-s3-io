package com.atlantbh.hadoop.s3.io;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class S3Utils {
	private static Logger LOG = LoggerFactory.getLogger(S3Utils.class);
	AmazonS3 s3client;

	public S3Utils() throws IOException {
		s3client = new AmazonS3Client(
				new PropertiesCredentials(S3Utils.class
						.getResourceAsStream("/AwsCredentials.properties")));
	}

	public List<S3ObjectSummary> getObjects(String bucketName, String prefix)
			throws IOException {

		if (bucketName == null || "".equals(bucketName)) {
			throw new IOException("S3 bucket name cannot be empty");
		}

		List<S3ObjectSummary> result = new ArrayList<S3ObjectSummary>();
		try {
			LOG.debug("Listing objects");

			ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
					.withBucketName(bucketName).withPrefix(prefix);

			ObjectListing objectListing;

			do {
				objectListing = s3client.listObjects(listObjectsRequest);
				for (S3ObjectSummary objectSummary : objectListing
						.getObjectSummaries()) {
					LOG.debug(" - {}  (size = {})", objectSummary.getKey(),
							objectSummary.getSize() + ")");
					result.add(objectSummary);
				}
				listObjectsRequest.setMarker(objectListing.getNextMarker());
			} while (objectListing.isTruncated());
		} catch (AmazonServiceException ase) {
			LOG.error("Error on AWS side: {}", ase.getMessage(), ase);
		} catch (AmazonClientException ace) {
			LOG.error("Error on client side: {}", ace.getMessage(), ace);
		}

		return result;
	}

	public byte[] getObject(String bucketName, String key) throws IOException {

		if (bucketName == null || "".equals(bucketName)) {
			throw new IOException("S3 bucket name cannot be empty");
		}

		GetObjectRequest request = new GetObjectRequest(bucketName, key);
		ByteArrayOutputStream baos = new ByteArrayOutputStream(8196);

		S3Object object = s3client.getObject(request);
		InputStream stream = object.getObjectContent();

		byte[] buffer = new byte[1024];
		int length = 0;
		
		while ((length = stream.read(buffer)) > 0) {
			baos.write(buffer, 0, length);
		}

		return baos.toByteArray();
	}

	public static void main(String[] args) throws IOException {

		S3Utils utils = new S3Utils();
		
		for (S3ObjectSummary object : utils.getObjects("pycsell_photos", "users/10/images_170_mk805v6z")) {
			byte[] buff = utils.getObject(object.getBucketName(), object.getKey());
			LOG.info("Content size={}", buff.length);
		};
	}
}
