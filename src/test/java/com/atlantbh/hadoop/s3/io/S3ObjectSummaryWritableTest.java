package com.atlantbh.hadoop.s3.io;

import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Date;

import org.junit.Assert;
import org.junit.Test;

import com.amazonaws.services.s3.model.Owner;

public class S3ObjectSummaryWritableTest {

	/**
	 * Tests S3Object serialization and deserialization
	 */
	@Test
	public void testReadWrite() {
		S3ObjectSummaryWritable objIn = new S3ObjectSummaryWritable();
		S3ObjectSummaryWritable objOut = new S3ObjectSummaryWritable();

		// serialized object
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(baos);

		// serialized object reader
		ByteArrayInputStream bais = null;
		DataInputStream dis = null;
		
		//Date
		Date date = new Date();
		
		//Owner
		Owner owner = new Owner("id", "displayName");

		objIn.setBucketName("bucketName");
		objIn.setKey("key");
		objIn.setETag("eTag");
		objIn.setLastModified(date);
		objIn.setSize(12345);
		objIn.setStorageClass("storageClass");
		objIn.setOwner(owner);

		try {
			objIn.write(dos);

			// create input reader
			bais = new ByteArrayInputStream(baos.toByteArray());
			dis = new DataInputStream(bais);

			objOut.readFields(dis);

			Assert.assertEquals(objIn.getBucketName(), objOut.getBucketName());
			Assert.assertEquals(objIn.getETag(), objOut.getETag());
			Assert.assertEquals(objIn.getLastModified(), objOut.getLastModified());
			Assert.assertEquals(objIn.getSize(), objOut.getSize());
			Assert.assertEquals(objIn.getStorageClass(), objOut.getStorageClass());
			Assert.assertEquals(objIn.getOwner(), objOut.getOwner());

		} catch (IOException e) {
			e.printStackTrace();
			fail("Exeception while writing data to out stream.");
		}
	}
}