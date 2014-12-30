/*
 * Copyright 2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.alertlogic.aws.analytics.poc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.alertlogic.aws.analytics.poc.Record;

/**
 * Sends Records to Amazon Kinesis.
 */
public class RecordKinesisPutter {
    private static final Log LOG = LogFactory.getLog(RecordKinesisPutter.class);

    private RecordFactory recordFactory;
    private AmazonKinesis kinesis;
    private String streamName;

    private final ObjectMapper JSON = new ObjectMapper();

    public RecordKinesisPutter(RecordFactory recordFactory, AmazonKinesis kinesis, String streamName) {
        if (recordFactory == null) {
            throw new IllegalArgumentException("recordFactory must not be null");
        }
        if (kinesis == null) {
            throw new IllegalArgumentException("kinesis must not be null");
        }
        if (streamName == null || streamName.isEmpty()) {
            throw new IllegalArgumentException("streamName must not be null or empty");
        }
        this.recordFactory = recordFactory;
        this.kinesis = kinesis;
        this.streamName = streamName;
    }

    /**
     * Send a fixed number of records to Amazon Kinesis. This sends them sequentially.
     * If you require more throughput consider using multiple {@link RecordKinesisPutter}s.
     *
     * @param n The number of records to send to Amazon Kinesis.
     * @param delayBetweenRecords The amount of time to wait in between sending records. If this is <= 0 it will be
     *        ignored.
     * @param unitForDelay The unit of time to interpret the provided delay as.
     *
     * @throws InterruptedException Interrupted while waiting to send the next record.
     */
    public void sendRecords(long n, long delayBetweenRecords, TimeUnit unitForDelay) throws InterruptedException {
        for (int i = 0; i < n && !Thread.currentThread().isInterrupted(); i++) {
            sendRecord();
            Thread.sleep(unitForDelay.toMillis(delayBetweenRecords));
        }
    }

    /**
     * Continuously sends records to Amazon Kinesis sequentially. This will only stop if interrupted. If you
     * require more throughput consider using multiple {@link RecordKinesisPutter}s.
     *
     * @param delayBetweenRecords The amount of time to wait in between sending records. If this is <= 0 it will be
     *        ignored.
     * @param unitForDelay The unit of time to interpret the provided delay as.
     *
     * @throws InterruptedException Interrupted while waiting to send the next record.
     */
    public void sendRecordsIndefinitely(long delayBetweenRecords, TimeUnit unitForDelay) throws InterruptedException {
        while (!Thread.currentThread().isInterrupted()) {
            sendRecord();
            if (delayBetweenRecords > 0) {
                Thread.sleep(unitForDelay.toMillis(delayBetweenRecords));
            }
        }
    }

    /**
     * Send a single record to Amazon Kinesis using PutRecord.
     */
    private void sendRecord() {
        Record record = recordFactory.create();
        byte[] bytes;
        try {
            bytes = JSON.writeValueAsBytes(record);
        } catch (IOException e) {
            LOG.warn("Skipping record. Unable to serialize: '" + record + "'", e);
            return;
        }

        PutRecordRequest putRecord = new PutRecordRequest();
        putRecord.setStreamName(streamName);
        // We use the resource as the partition key so we can accurately calculate totals for a given resource
        putRecord.setPartitionKey(record.getField("resource"));
        putRecord.setData(ByteBuffer.wrap(bytes));
        // Order is not important for this application so we do not send a SequenceNumberForOrdering
        putRecord.setSequenceNumberForOrdering(null);

        try {
            kinesis.putRecord(putRecord);
        } catch (ProvisionedThroughputExceededException ex) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("Thread %s's Throughput exceeded. Waiting 10ms", Thread.currentThread().getName()));
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        } catch (AmazonClientException ex) {
            LOG.warn("Error sending record to Amazon Kinesis.", ex);
        }
    }
}
