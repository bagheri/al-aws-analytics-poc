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

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import com.alertlogic.aws.analytics.poc.Record;

/**
 * Generates random {@link Record}s based on an internal sample set. This class is thread safe.
 */
public class RecordFactory {
    private List<String> resources;
    private List<String> records;

    /**
     * Create a new generator which will use the resources and records provided.
     *
     * @param resources List of resources to use when generating a record.
     * @param records List of records to use when generating a record.
     */
    public RecordFactory(List<String> resources, List<String> records) {
        if (resources == null || resources.isEmpty()) {
            throw new IllegalArgumentException("At least 1 resource is required");
        }
        if (records == null || records.isEmpty()) {
            throw new IllegalArgumentException("At least 1 record is required");
        }
        this.resources = resources;
        this.records = records;
    }

    /**
     * Creates a new record record using random resources and records from the collections provided when this
     * factory was created.
     *
     * @return A new record with random resource and record values.
     */
    public Record create() {
        String resource = getRandomResource();
        String field = getRandomField();

        Record record = new Record(resource, field);

        return record;
    }

    /**
     * Gets a random resource from the collection of resources.
     *
     * @return A random resource.
     */
    protected String getRandomResource() {
        return resources.get(ThreadLocalRandom.current().nextInt(resources.size()));
    }

    /**
     * Gets a random record from the collection of records.
     *
     * @return A random record.
     */
    protected String getRandomField() {
        return records.get(ThreadLocalRandom.current().nextInt(records.size()));
    }

}
