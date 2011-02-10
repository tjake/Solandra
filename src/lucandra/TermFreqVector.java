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

import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.SliceByNamesReadCommand;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.utils.ByteBufferUtil;
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

        List<Row> rows = CassandraUtils.robustRead(CassandraUtils.consistency, rc);
       
        if (rows.isEmpty()){
            
            return; // this docId is missing
        }
        
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

            readCommands.add(new SliceByNamesReadCommand(CassandraUtils.keySpace, key, new ColumnParent().setColumn_family(CassandraUtils.termVecColumnFamily), Arrays.asList(ByteBuffer.wrap(CassandraUtils.writeVInt(docI)))));
        }

        rows = CassandraUtils.robustRead(CassandraUtils.consistency, readCommands.toArray(new ReadCommand[]{}));

        terms = new String[rows.size()];
        freqVec = new int[rows.size()];
        termPositions = new int[rows.size()][];
        termOffsets = new TermVectorOffsetInfo[rows.size()][];

        int i = 0;

        for (Row row : rows) {
            String rowKey = ByteBufferUtil.string(row.key.key,CassandraUtils.UTF_8);
           
            String termStr = rowKey.substring(rowKey.indexOf(CassandraUtils.delimeter) + CassandraUtils.delimeter.length());

            Term t = CassandraUtils.parseTerm(termStr);

            terms[i] = t.text();

            // Find the offsets and positions
            LucandraTermInfo termInfo = null;
            
            if(row.cf != null){
                termInfo = new LucandraTermInfo(0, row.cf.getSortedColumns().iterator().next().value());
                
                termPositions[i] = termInfo.positions;
            }
            
            freqVec[i] = termPositions[i].length;

            if (termInfo == null || !termInfo.hasOffsets) {
                termOffsets[i] = TermVectorOffsetInfo.EMPTY_OFFSET_INFO;
            } else {

                int[] offsets = termInfo.offsets;

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
