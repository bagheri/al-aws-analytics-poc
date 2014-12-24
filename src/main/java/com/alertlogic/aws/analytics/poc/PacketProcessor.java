/*
 * Copyright 2014 Alert Logic, Inc. or its affiliates. All Rights Reserved.
 *
 */

package com.alertlogic.aws.analytics.poc;

import java.net.UnknownHostException;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;

import com.alertlogic.aws.analytics.poc.CountingRecordProcessorFactory;
import com.alertlogic.aws.analytics.poc.CountPersister;
import com.alertlogic.aws.analytics.poc.DynamoDBCountPersister;
import com.alertlogic.aws.analytics.poc.Record;
import com.alertlogic.aws.analytics.poc.DynamoDBUtils;
import com.alertlogic.aws.analytics.poc.SampleUtils;
import com.alertlogic.aws.analytics.poc.StreamUtils;

/**
 * Amazon Kinesis stream-processing application to test analytics.
 */
public class PacketProcessor {
    private static final Log LOG = LogFactory.getLog(PacketProcessor.class);

    // Count occurrences of records over a range of 10 seconds
    private static final int COMPUTE_RANGE_FOR_COUNTS_IN_MILLIS = 10000;
    // Update the counts every 1 second
    private static final int COMPUTE_INTERVAL_IN_MILLIS = 1000;

    /**
     * Start the Kinesis Client application.
     * 
     * @param args Application name to use for the Kinesis Client Application,
     *             Stream name to read from,
     *             DynamoDB table name to write accumulated data into,
     *             and the AWS region for resources.
     */
    public static void main(String[] args) throws UnknownHostException {
        if (args.length != 4) {
            System.err.println("Usage: "
                               + PacketProcessor.class.getSimpleName()
                               + " <application name> <stream name>"
                               + " <DynamoDB table name> <region>");
            System.exit(1);
        }

        String applicationName = args[0];
        String streamName = args[1];
        String countsTableName = args[2];
        Region region = SampleUtils.parseRegion(args[3]);

        AWSCredentialsProvider credentialsProvider =
            new DefaultAWSCredentialsProviderChain();
        ClientConfiguration clientConfig =
            SampleUtils.configureUserAgentForSample(new ClientConfiguration());
        AmazonKinesis kinesis =
            new AmazonKinesisClient(credentialsProvider, clientConfig);
        kinesis.setRegion(region);
        AmazonDynamoDB dynamoDB =
            new AmazonDynamoDBClient(credentialsProvider, clientConfig);
        dynamoDB.setRegion(region);

        // Creates a stream to write to, if it doesn't exist
        StreamUtils streamUtils = new StreamUtils(kinesis);
        streamUtils.createStreamIfNotExists(streamName, 2);
        LOG.info(String.format("%s stream is ready for use", streamName));

        DynamoDBUtils dynamoDBUtils = new DynamoDBUtils(dynamoDB);
        dynamoDBUtils.createCountTableIfNotExists(countsTableName);
        LOG.info(String.format("%s DynamoDB table is ready for use",
                               countsTableName));

        String workerId = String.valueOf(UUID.randomUUID());
        LOG.info(String.format("Using working id: %s", workerId));
        KinesisClientLibConfiguration kclConfig =
                new KinesisClientLibConfiguration(applicationName,
                                                  streamName,
                                                  credentialsProvider,
                                                  workerId);
        kclConfig.withCommonClientConfig(clientConfig);
        kclConfig.withRegionName(region.getName());
        kclConfig.withInitialPositionInStream(InitialPositionInStream.LATEST);

        // Persist counts to DynamoDB
        DynamoDBCountPersister persister =
                new DynamoDBCountPersister(
                        dynamoDBUtils.createMapperForTable(countsTableName));

        IRecordProcessorFactory recordProcessor =
                new CountingRecordProcessorFactory<Record>(
                        Record.class,
                        persister,
                        COMPUTE_RANGE_FOR_COUNTS_IN_MILLIS,
                        COMPUTE_INTERVAL_IN_MILLIS);

        Worker worker = new Worker(recordProcessor, kclConfig);

        int exitCode = 0;
        try {
            worker.run();
        } catch (Throwable t) {
            LOG.error("Caught throwable while processing data.", t);
            exitCode = 1;
        }
        System.exit(exitCode);
    }
}
