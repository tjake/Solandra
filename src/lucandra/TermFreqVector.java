/**
 * Copyright T Jake Luciani
 * 
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
package lucandra;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.SliceByNamesReadCommand;
import org.apache.cassandra.db.SliceFromReadCommand;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermVectorOffsetInfo;

public class TermFreqVector implements org.apache.lucene.index.TermFreqVector, org.apache.lucene.index.TermPositionVector {

    private String field;
    private byte[] docId;
    private String[] terms;
    private int[] freqVec;
    private int[][] termPositions;
    private TermVectorOffsetInfo[][] termOffsets;

    public TermFreqVector(String indexName, String field, int docI) {
        this.field = field;
        this.docId = Integer.toHexString(docI).getBytes();

        ByteBuffer key = CassandraUtils.hashKeyBytes(indexName.getBytes(), CassandraUtils.delimeterBytes, docId );

        ReadCommand rc = new SliceByNamesReadCommand(CassandraUtils.keySpace, key, CassandraUtils.metaColumnPath, Arrays
                .asList(CassandraUtils.documentMetaFieldBytes));

        List<Row> rows = null;
        int attempts = 0;
        while (attempts++ < 10) {
            try {
                rows = StorageProxy.readProtocol(Arrays.asList(rc), ConsistencyLevel.ONE);
                break;
            } catch (IOException e1) {
                throw new RuntimeException(e1);
            } catch (UnavailableException e1) {
                
            } catch (TimeoutException e1) {
                
            } catch (InvalidRequestException e) {
                throw new RuntimeException(e);
            }

            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {

            }
        }

        if (attempts >= 10)
            throw new RuntimeException("Read command failed after 10 attempts");

        if (rows.isEmpty())
            return; // nothing to delete

        List<Term> allTerms;
        try {
            allTerms = (List<Term>) CassandraUtils.fromBytes(rows.get(0).cf.getColumn(CassandraUtils.documentMetaFieldBytes).value());
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        List<ReadCommand> readCommands = new ArrayList<ReadCommand>();

        for (Term t : allTerms) {

            // skip the ones not of this field
            if (!t.field().equals(field))
                continue;

            // add to multiget params
            try {
                key = CassandraUtils.hashKeyBytes(indexName.getBytes(),  CassandraUtils.delimeterBytes, t.field().getBytes("UTF-8"), CassandraUtils.delimeterBytes, t.text().getBytes("UTF-8"));
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException("JVM doesn't support UTF-8",e);
            }

            readCommands.add(new SliceFromReadCommand(CassandraUtils.keySpace, key, new ColumnParent().setColumn_family(CassandraUtils.termVecColumnFamily)
                    .setSuper_column( CassandraUtils.writeVInt(docI)), FBUtilities.EMPTY_BYTE_BUFFER, FBUtilities.EMPTY_BYTE_BUFFER, false, 1024));
        }

        try {
            rows = StorageProxy.readProtocol(readCommands, ConsistencyLevel.ONE);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (UnavailableException e) {
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        } catch (InvalidRequestException e) {
            throw new RuntimeException(e);
        }

        terms = new String[rows.size()];
        freqVec = new int[rows.size()];
        termPositions = new int[rows.size()][];
        termOffsets = new TermVectorOffsetInfo[rows.size()][];

        int i = 0;

        for (Row row : rows) {
            String rowKey;
            try {
                rowKey = new String(row.key.key.array(),row.key.key.position(),row.key.key.remaining(),"UTF-8");
            } catch (UnsupportedEncodingException e) {
               throw new RuntimeException("JVM does not support UTF-8");
            }
            String termStr = rowKey.substring(rowKey.indexOf(CassandraUtils.delimeter) + CassandraUtils.delimeter.length());

            Term t = CassandraUtils.parseTerm(termStr);

            terms[i] = t.text();

            // Find the offsets and positions
            IColumn positionVector = null;
            IColumn offsetVector   = null;
            
            if(row.cf != null){
                positionVector = row.cf.getSortedColumns().iterator().next().getSubColumn(CassandraUtils.positionVectorKeyBytes);
                offsetVector   = row.cf.getSortedColumns().iterator().next().getSubColumn(CassandraUtils.offsetVectorKeyBytes);
            }
            
            termPositions[i] = positionVector == null ? new int[] {} : CassandraUtils.byteArrayToIntArray(positionVector.value());
            freqVec[i] = termPositions[i].length;

            if (offsetVector == null) {
                termOffsets[i] = TermVectorOffsetInfo.EMPTY_OFFSET_INFO;
            } else {

                int[] offsets = CassandraUtils.byteArrayToIntArray(offsetVector.value());

                termOffsets[i] = new TermVectorOffsetInfo[freqVec[i]];
                for (int j = 0, k = 0; j < offsets.length; j += 2, k++) {
                    termOffsets[i][k] = new TermVectorOffsetInfo(offsets[j], offsets[j + 1]);
                }
            }

            i++;
        }

    }

    public String getField() {
        return field;
    }

    public int[] getTermFrequencies() {
        return freqVec;
    }

    public String[] getTerms() {
        return terms;
    }

    public int indexOf(String term) {
        return Arrays.binarySearch(terms, term);
    }

    public int[] indexesOf(String[] terms, int start, int len) {
        int[] res = new int[terms.length];

        for (int i = 0; i < terms.length; i++) {
            res[i] = indexOf(terms[i]);
        }

        return res;
    }

    public int size() {
        return terms.length;
    }

    public TermVectorOffsetInfo[] getOffsets(int index) {
        return termOffsets[index];
    }

    public int[] getTermPositions(int index) {
        return termPositions[index];
    }

}
