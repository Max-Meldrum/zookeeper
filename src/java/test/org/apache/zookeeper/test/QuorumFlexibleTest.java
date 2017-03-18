/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.test;
import java.util.HashSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.server.quorum.Leader;
import org.apache.zookeeper.server.quorum.flexible.*;
import org.apache.zookeeper.server.quorum.Leader.Proposal;
import org.junit.Assert;
import org.junit.Test;

public class QuorumFlexibleTest extends QuorumBase {
    protected static final Logger LOG = LoggerFactory.getLogger(QuorumFlexibleTest.class);
    public static final long CONNECTION_TIMEOUT = ClientTest.CONNECTION_TIMEOUT;

    /***************************************************************/
    /* Test that the Flexible quorum verifier only counts votes from */
    /* followers in its view                                    */
    /***************************************************************/
    @Test
    public void testFlexibleQuorums() throws Throwable {
        //setup servers 1-5 to be followers
        setUp(false);

        Proposal p = new Proposal();
        p.addQuorumVerifier(s1.getQuorumVerifier());

        /**
         * N = 5
         * Q1 = Leader Election, Q2 = Atomic Broadcast
         * Q1 = 4, Q2 = 2
         * Q1 + Q2 > N
         */

        p.addAck(Long.valueOf(1));
        Assert.assertEquals(false, p.hasAllAtomicBroadcastQuorums());
        p.addAck(Long.valueOf(2));
        Assert.assertEquals(true, p.hasAllAtomicBroadcastQuorums());

        // Not enough for a leader election yet
        Assert.assertEquals(false, p.hasAllElectionQuorums());

        p.addAck(Long.valueOf(3));
        p.addAck(Long.valueOf(4));

        // Now there is enough for Q1
        Assert.assertEquals(true, p.hasAllElectionQuorums());

    }
}
