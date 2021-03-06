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

import java.util.Date;
import java.util.List;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMarshalling;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.alertlogic.aws.analytics.poc.FieldCountMarshaller;

/**
 * A record and the number of occurrences of a field by resource over a given period of time.
 */
@DynamoDBTable(tableName = "ALAnalyticsPOC-NameToBeReplacedByDynamoDBMapper")
public class RecordCount {
    private String resource;
    // The timestamp when the counts were calculated
    private Date timestamp;
    // Store the hostname of the worker that updated the count
    private String host;
    // Ordered list of field counts in descending order. Top N can be simply obtained by inspecting the first N
    // counts.
    private List<FieldCount> fieldCounts;

    @DynamoDBHashKey
    public String getResource() {
        return resource;
    }

    public void setResource(String resource) {
        this.resource = resource;
    }

    @DynamoDBRangeKey
    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

    @DynamoDBAttribute
    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    @DynamoDBAttribute
    @DynamoDBMarshalling(marshallerClass = FieldCountMarshaller.class)
    public List<FieldCount> getFieldCounts() {
        return fieldCounts;
    }

    public void setFieldCounts(List<FieldCount> fieldCounts) {
        this.fieldCounts = fieldCounts;
    }
}
