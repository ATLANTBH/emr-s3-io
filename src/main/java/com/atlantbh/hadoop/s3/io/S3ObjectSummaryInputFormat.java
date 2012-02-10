package com.atlantbh.hadoop.s3.io;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * S3 object summary input format
 * 
 * @author seljaz
 *
 */
public class S3ObjectSummaryInputFormat extends S3InputFormat<Text, S3ObjectSummaryWritable>  {

	public S3ObjectSummaryInputFormat() throws IOException {
		super();
	}

	@Override
	public RecordReader<Text, S3ObjectSummaryWritable> createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new S3ObjectSummaryRecordReader();
	}
}
