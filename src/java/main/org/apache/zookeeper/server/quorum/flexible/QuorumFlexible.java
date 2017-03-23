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

/**
 * Modifications copyright (C) 2017 <Max Meldrum>
 */


package org.apache.zookeeper.server.quorum.flexible;

import java.util.HashMap;
import java.util.Set;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;

import org.apache.zookeeper.server.quorum.QuorumPeer.LearnerType;
import org.apache.zookeeper.server.quorum.QuorumPeer.QuorumServer;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;

/**
 * This class implements a validator for Flexible (FPaxos) quorums.
 */
public class QuorumFlexible implements QuorumVerifier {
    private Map<Long, QuorumServer> allMembers = new HashMap<Long, QuorumServer>();
    private HashMap<Long, QuorumServer> votingMembers = new HashMap<Long, QuorumServer>();
    private HashMap<Long, QuorumServer> observingMembers = new HashMap<Long, QuorumServer>();
    private long version = 0;
    private int half;
    private String quorumSystem = "Flexible";
    /**
     * Default quorums to a 5 node ensemble,
     * Q1 = 4, Q2 = 2
     * Following ZooKeeper convention and using ">" instead of ">="
     * So Q1 turns into 3 and Q2 to 1
     */
    private int electionQuorum = 3;
    private int atomicBroadcastQuorum = 1;

    public int hashCode() {
        assert false : "hashCode not designed";
        return 42; // any arbitrary constant will do
    }

    public boolean equals(Object o) {
        if (!(o instanceof QuorumFlexible)) {
            return false;
        }
        QuorumFlexible qf = (QuorumFlexible) o;
        if (qf.getVersion() == version)
            return true;
        if (allMembers.size() != qf.getAllMembers().size())
            return false;
        for (QuorumServer qs : allMembers.values()) {
            QuorumServer qso = qf.getAllMembers().get(qs.id);
            if (qso == null || !qs.equals(qso))
                return false;
        }
        return true;
    }

    /**
     * Defines a majority to avoid computing it every time.
     * <Max Meldrum> + sets values for Q1 and Q2
     */
    public QuorumFlexible(Map<Long, QuorumServer> allMembers) {
        this.allMembers = allMembers;
        for (QuorumServer qs : allMembers.values()) {
            if (qs.type == LearnerType.PARTICIPANT) {
                votingMembers.put(Long.valueOf(qs.id), qs);
            } else {
                observingMembers.put(Long.valueOf(qs.id), qs);
            }
        }
        half = votingMembers.size() / 2;
        setQuorumValues(votingMembers.size());
    }

    public QuorumFlexible(Properties props) throws ConfigException {
        for (Entry<Object, Object> entry : props.entrySet()) {
            String key = entry.getKey().toString();
            String value = entry.getValue().toString();

            if (key.startsWith("server.")) {
                int dot = key.indexOf('.');
                long sid = Long.parseLong(key.substring(dot + 1));
                QuorumServer qs = new QuorumServer(sid, value);
                allMembers.put(Long.valueOf(sid), qs);
                if (qs.type == LearnerType.PARTICIPANT)
                    votingMembers.put(Long.valueOf(sid), qs);
                else {
                    observingMembers.put(Long.valueOf(sid), qs);
                }
            } else if (key.equals("version")) {
                version = Long.parseLong(value, 16);
            } else if (key.startsWith("leaderElectionQuorum")) {
                electionQuorum = Integer.parseInt(value);
            } else if (key.startsWith("atomicBroadcastQuorum")) {
                atomicBroadcastQuorum = Integer.parseInt(value);
            }
        }
        half = votingMembers.size() / 2;
    }

    /**
     * Sets Quorum for Leader Election and Atomic Broadcast
     * if we don't specifiy it in zookeeper config
     * <Max Meldrum>
     */
    private void setQuorumValues(int votingMembers) {
        if (votingMembers == 5) {
            /**
             * N = 5, Q1 = 4 , Q2 = 2
             * Following ZooKeeper convention and using ">" instead of ">="
             * So Q1 turns into 3 and Q2 to 1
             */
            electionQuorum = 3;
            atomicBroadcastQuorum = 1;
        } else if (votingMembers == 7) {
            /**
             * N = 7, Q1 = 6, Q2 = 2
             * Following ZooKeeper convention and using ">" instead of ">="
             * So Q1 turns into 5 and Q2 to 1
             */
            electionQuorum = 5;
            atomicBroadcastQuorum = 1;
        } else {
            // Else just go with majority
            electionQuorum = (votingMembers / 2);
            atomicBroadcastQuorum = (votingMembers /2);
        }
    }

    /**
     * Returns weight of 1 by default.
     *
     * @param id
     */
    public long getWeight(long id) {
        return (long) 1;
    }

    public String toString() {
        StringBuilder sw = new StringBuilder();

        for (QuorumServer member : getAllMembers().values()) {
            String key = "server." + member.id;
            String value = member.toString();
            sw.append(key);
            sw.append('=');
            sw.append(value);
            sw.append('\n');
        }
        String hexVersion = Long.toHexString(version);
        sw.append("version=");
        sw.append(hexVersion);
        return sw.toString();
    }

    public String getQuorumSystem() {
        return quorumSystem;
    }

    /**
     * Verifies if a set is a majority. Assumes that ackSet contains acks only
     * from votingMembers
     */
    public boolean containsQuorum(Set<Long> ackSet) {
        return (ackSet.size() > half);
    }

    /**
     * Verifies if a set has enough quorum for Leader Election
     * <Max Meldrum>
     */
    public boolean containsElectionQuorum(Set<Long> ackSet) {
        return (ackSet.size() > electionQuorum);
    }

    /**
     * Verifies if a set has enough quorum for Atomic Broadcast
     * <Max Meldrum>
     */
    public boolean containsAtomicBroadcastQuorum(Set<Long> ackSet) {
        return (ackSet.size() > atomicBroadcastQuorum);
    }


    public Map<Long, QuorumServer> getAllMembers() {
        return allMembers;
    }

    public Map<Long, QuorumServer> getVotingMembers() {
        return votingMembers;
    }

    public Map<Long, QuorumServer> getObservingMembers() {
        return observingMembers;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long ver) {
        version = ver;
    }
}