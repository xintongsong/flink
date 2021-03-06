/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kinesis.testutils;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;

import com.amazonaws.kinesis.agg.AggRecord;
import com.amazonaws.kinesis.agg.RecordAggregator;
import com.amazonaws.services.kinesis.model.Record;
import org.apache.commons.lang3.RandomStringUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * General test utils.
 */
public class TestUtils {
	/**
	 * Get standard Kinesis-related config properties.
	 */
	public static Properties getStandardProperties() {
		Properties config = new Properties();
		config.setProperty(AWSConfigConstants.AWS_REGION, "us-east-1");
		config.setProperty(AWSConfigConstants.AWS_ACCESS_KEY_ID, "accessKeyId");
		config.setProperty(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "secretKey");

		return config;
	}

	/**
	 * Creates a batch of {@code numOfAggregatedRecords} aggregated records.
	 * Each aggregated record contains {@code numOfChildRecords} child records.
	 * Each record is assigned the sequence number: {@code sequenceNumber + index * numOfChildRecords}.
	 * The next sequence number is output to the {@code sequenceNumber}.
	 *
	 * @param numOfAggregatedRecords the number of records in the batch
	 * @param numOfChildRecords the number of child records for each aggregated record
	 * @param sequenceNumber the starting sequence number, outputs the next sequence number
	 * @return the batch af aggregated records
	 */
	public static List<Record> createAggregatedRecordBatch(
			final int numOfAggregatedRecords,
			final int numOfChildRecords,
			final AtomicInteger sequenceNumber) {
		List<Record> recordBatch = new ArrayList<>();
		RecordAggregator recordAggregator = new RecordAggregator();

		for (int record = 0; record < numOfAggregatedRecords; record++) {
			String partitionKey = UUID.randomUUID().toString();

			for (int child = 0; child < numOfChildRecords; child++) {
				byte[] data = RandomStringUtils.randomAlphabetic(1024)
					.getBytes(ConfigConstants.DEFAULT_CHARSET);

				try {
					recordAggregator.addUserRecord(partitionKey, data);
				} catch (Exception e) {
					throw new IllegalStateException("Error aggregating message", e);
				}
			}

			AggRecord aggRecord = recordAggregator.clearAndGet();

			recordBatch.add(new Record()
				.withData(ByteBuffer.wrap(aggRecord.toRecordBytes()))
				.withPartitionKey(partitionKey)
				.withApproximateArrivalTimestamp(new Date(System.currentTimeMillis()))
				.withSequenceNumber(String.valueOf(sequenceNumber.getAndAdd(numOfChildRecords))));
		}

		return recordBatch;
	}

}
