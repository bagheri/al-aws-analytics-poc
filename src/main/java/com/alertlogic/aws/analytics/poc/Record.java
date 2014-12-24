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

package com.alertlogic.aws.kinesis.test1.model;

/**
 * Standin for a general record.
 */
public class Record {
    private String resource;
    private String referrer;

    public Record() {
    }

    public Record(String resource, String referrer) {
        this.resource = resource;
        this.referrer = referrer;
    }

    public String getField(String name) {
        switch (name)
        {
            case "resource": return resource;
            case "referrer": return referrer;
        }
        return "";
    }

    public void setField(String name, String value) {
        switch (name)
        {
            case "resource": return this.resource = value;
            case "referrer": return this.referrer = referrer;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Record that = (Record) o;

        if (referrer != null ? !referrer.equals(that.referrer) : that.referrer != null) return false;
        if (resource != null ? !resource.equals(that.resource) : that.resource != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = resource != null ? resource.hashCode() : 0;
        result = 31 * result + (referrer != null ? referrer.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Record{" +
                "resource='" + resource + '\'' +
                ", referrer='" + referrer + '\'' +
                '}';
    }
}
