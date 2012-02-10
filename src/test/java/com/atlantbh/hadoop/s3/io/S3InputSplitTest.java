package com.atlantbh.hadoop.s3.io;

import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.junit.Test;

import junit.framework.Assert;


/**
 * S3 Input split class test
 * 
 * @author seljaz
 *
 */
public class S3InputSplitTest {
	/**
	 * Tests S3 Input split serialization and deserialization
	 */
	@Test
	public void testReadWrite() {
		S3InputSplit objIn = new S3InputSplit();
		S3InputSplit objOut = new S3InputSplit();

		objIn.setBucketName("bucketName");
		objIn.setKeyPrefix("key prefix");
		objIn.setLastKey("last Key");
		objIn.setMarker("marker");
		objIn.setSize(56);

		try {
			// serialize input object
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutputStream dos = new DataOutputStream(baos);
			
			objIn.write(dos);
			
			// deserialize into output object
			ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
			DataInputStream dis = new DataInputStream(bais);

			objOut.readFields(dis);

			Assert.assertEquals(objIn.getBucketName(), objOut.getBucketName());
			Assert.assertEquals(objIn.getLastKey(), objOut.getLastKey());
			Assert.assertEquals(objIn.getMarker(), objOut.getMarker());
			Assert.assertEquals(objIn.getSize(), objOut.getSize());
			
		} catch (IOException e) {
			e.printStackTrace();
			fail("Exeception while writing data to out stream.");
		}
	}
	
}
