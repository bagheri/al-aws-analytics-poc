/*
 * Copyright 2014 Alert Logic Inc. or its affiliates. All Rights Reserved.
 *
 */

package com.alertlogic.aws.analytics.poc;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;

/**
 * A command-line tool that reads packets and streams them to Kinesis.
 */
public class PacketStreamer {
    private static final Log LOG = LogFactory.getLog(PacketStreamer.class);

    /**
     * The amount of time to wait between records.
     *
     * This is a way to control usage costs.
     */
    private static final long DELAY_BETWEEN_RECORDS_IN_MILLIS = 100;

    /**
     * Start a number of threads that read packets and sends them to
     * a Kinesis Stream.
     *
     * @param args the number of threads to use,
     *        the name of the Kinesis stream to send to,
     *        the AWS region for the resources that exist or should be created.
     *
     * @throws InterruptedException If this application is interrupted
     */
    public static void main(String[] args) throws InterruptedException {
        if (args.length != 3) {
            System.err.println("Usage: " + PacketStreamer.class.getSimpleName()
                    + " <number of threads> <stream name> <region>");
            System.exit(1);
        }

        int numberOfThreads = Integer.parseInt(args[0]);
        String streamName = args[1];
        Region region = SampleUtils.parseRegion(args[2]);

        AWSCredentialsProvider credentialsProvider =
            new DefaultAWSCredentialsProviderChain();

        ClientConfiguration clientConfig =
            SampleUtils.configureUserAgentForSample(new ClientConfiguration());

        AmazonKinesis kinesis =
            new AmazonKinesisClient(credentialsProvider, clientConfig);

        kinesis.setRegion(region);

        List<String> resources = new ArrayList<>();
        resources.add("/index.html");

        // These are the possible fields to use when generating records
        List<String> fields = new ArrayList<>();
        fields.add("http://www.amazon.com");
        fields.add("http://www.google.com");
        fields.add("http://www.yahoo.com");
        fields.add("http://www.bing.com");
        fields.add("http://www.stackoverflow.com");
        fields.add("http://www.reddit.com");

        RecordFactory recordFactory = new RecordFactory(resources, fields);

        // Creates a stream to write to with 2 shards if it doesn't exist
        StreamUtils streamUtils = new StreamUtils(kinesis);
        streamUtils.createStreamIfNotExists(streamName, 2);
        LOG.info(String.format("%s stream is ready for use", streamName));

        final RecordKinesisPutter putter = new RecordKinesisPutter(recordFactory, kinesis, streamName);

        ExecutorService es = Executors.newCachedThreadPool();

        Runnable recordSender = new Runnable() {
            @Override
            public void run() {
                try {
                    putter.sendPairsIndefinitely(DELAY_BETWEEN_RECORDS_IN_MILLIS, TimeUnit.MILLISECONDS);
                } catch (Exception ex) {
                    LOG.warn("Thread encountered an error while sending records. Records will no longer be put by this thread.",
                            ex);
                }
            }
        };

        for (int i = 0; i < numberOfThreads; i++) {
            es.submit(recordSender);
        }

        LOG.info(String.format("Sending records with a %dms delay between records with %d thread(s).",
                DELAY_BETWEEN_RECORDS_IN_MILLIS,
                numberOfThreads));

        es.shutdown();
        es.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
    }
}
