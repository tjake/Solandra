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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.db.IColumn;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.OpenBitSet;

public class LucandraFilter extends Filter {

    private static final long serialVersionUID = 1L;

    private List<Term> terms = new ArrayList<Term>();

    public void addTerm(Term term) {
        terms.add(term);
    }

    public DocIdSet getDocIdSet(IndexReader reader) throws IOException {
        OpenBitSet result = new OpenBitSet(reader.maxDoc());

        OpenBitSet docsHit = ((lucandra.IndexReader) reader).getDocsHit();
        
       
        List<ByteBuffer> filteredValues = new ArrayList<ByteBuffer>();
        for(int i=0; i<docsHit.capacity(); i++){          
            if(docsHit.fastGet(i))
                filteredValues.add(ByteBuffer.wrap(CassandraUtils.writeVInt(i)));
        }

        if (filteredValues.size() == 0)
            return null;

        LucandraTermDocs termDocs = (LucandraTermDocs) reader.termDocs();

        for (Term term : terms) {
            LucandraTermInfo[] terms = termDocs.filteredSeek(term, filteredValues);
            // This is a conjunction and at least one value must match
            if (terms == null)
                return null;

            while (termDocs.next()) {
                result.set(termDocs.doc());
            }
        }
        
        termDocs.close();
        return result;
    }
}
