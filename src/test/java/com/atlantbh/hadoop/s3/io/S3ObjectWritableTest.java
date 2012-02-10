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

import com.amazonaws.services.s3.model.ObjectMetadata;

public class S3ObjectWritableTest {

	/**
	 * Tests S3Object serialization and de serialization
	 */
	@Test
	public void testReadWrite() {
		S3ObjectWritable objIn = new S3ObjectWritable();
		S3ObjectWritable objOut = new S3ObjectWritable();

		// content
		byte[] contentBuffer = new byte[]{20, 30, 40, 50};
		ByteArrayInputStream contentIs = new ByteArrayInputStream(contentBuffer);

		// serialized object
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(baos);

		// serialized object reader
		ByteArrayInputStream bais = null;
		DataInputStream dis = null;

		objIn.setBucketName("bucketName");
		objIn.setKey("key");
		
		// object metadata
		objIn.setObjectMetadata(new ObjectMetadata());
		objIn.getObjectMetadata().setContentLength(4l);
		objIn.getObjectMetadata().setCacheControl("cacheControl");
		objIn.getObjectMetadata().setContentDisposition("disposition");
		objIn.getObjectMetadata().setContentEncoding("encoding");
		objIn.getObjectMetadata().setContentMD5("md5Base64");
		objIn.getObjectMetadata().setContentType("contentType");
		objIn.getObjectMetadata().setLastModified(new Date());
		objIn.getObjectMetadata().setServerSideEncryption("serverSideEncryption");
		
		objIn.setObjectContent(contentIs);

		try {
			objIn.write(dos);

			// create input reader
			bais = new ByteArrayInputStream(baos.toByteArray());
			dis = new DataInputStream(bais);

			objOut.readFields(dis);

			Assert.assertEquals(objIn.getBucketName(), objOut.getBucketName());
			Assert.assertEquals(objIn.getKey(), objOut.getKey());
			Assert.assertEquals(objIn.getObjectMetadata().getContentLength(), objOut.getObjectMetadata().getContentLength());
			Assert.assertEquals(objIn.getObjectMetadata().getCacheControl(), objOut.getObjectMetadata().getCacheControl());
			Assert.assertEquals(objIn.getObjectMetadata().getContentDisposition(), objOut.getObjectMetadata().getContentDisposition());
			Assert.assertEquals(objIn.getObjectMetadata().getContentEncoding(), objOut.getObjectMetadata().getContentEncoding());
			Assert.assertEquals(objIn.getObjectMetadata().getContentMD5(), objOut.getObjectMetadata().getContentMD5());
			Assert.assertEquals(objIn.getObjectMetadata().getContentType(), objOut.getObjectMetadata().getContentType());
			Assert.assertEquals(objIn.getObjectMetadata().getServerSideEncryption(), objOut.getObjectMetadata().getServerSideEncryption());
			Assert.assertEquals(objIn.getObjectMetadata().getLastModified(), objOut.getObjectMetadata().getLastModified());

		} catch (IOException e) {
			e.printStackTrace();
			fail("Exeception while writing data to out stream.");
		}
	}
}