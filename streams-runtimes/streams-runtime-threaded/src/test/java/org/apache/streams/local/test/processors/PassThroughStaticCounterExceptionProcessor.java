/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.streams.local.test.processors;

import org.apache.streams.core.StreamsDatum;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;


public class PassThroughStaticCounterExceptionProcessor extends PassThroughStaticCounterProcessor{
    private final int numErrorsToThrow;
    private final AtomicInteger numErrorsThrown = new AtomicInteger(0);

    public PassThroughStaticCounterExceptionProcessor(int delay, int numErrorsToThrow) {
        super(delay);
        this.numErrorsToThrow = numErrorsToThrow <= 0 ? 1 : numErrorsToThrow;
    }

    @Override
    public List<StreamsDatum> process(StreamsDatum entry) {
        super.sleepSafely();
        super.count.incrementAndGet();
        List<StreamsDatum> result = new LinkedList<StreamsDatum>();

        if (this.numErrorsThrown.getAndIncrement() < this.numErrorsToThrow) {
            throw new RuntimeException();
        } else {
            result.add(entry);
        }

        return result;
    }
}
