/**
 * Copyright 2010 T Jake Luciani
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
package solandra;

import java.io.IOException;
import java.net.URL;
import java.util.concurrent.atomic.AtomicLong;

import lucandra.CassandraUtils;
import lucandra.cluster.AbstractIndexManager;
import lucandra.cluster.RedisIndexManager;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.SolrCore;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.update.MergeIndexesCommand;
import org.apache.solr.update.RollbackUpdateCommand;
import org.apache.solr.update.UpdateHandler;

public class SolandraIndexWriter extends UpdateHandler {

    private final lucandra.IndexWriter writer;
    private final RedisIndexManager    indexManager;
    
    // stats
    AtomicLong addCommands = new AtomicLong();
    AtomicLong addCommandsCumulative = new AtomicLong();
    AtomicLong deleteByIdCommands = new AtomicLong();
    AtomicLong deleteByIdCommandsCumulative = new AtomicLong();
    AtomicLong deleteByQueryCommands = new AtomicLong();
    AtomicLong deleteByQueryCommandsCumulative = new AtomicLong();
    AtomicLong expungeDeleteCommands = new AtomicLong();
    AtomicLong mergeIndexesCommands = new AtomicLong();
    AtomicLong commitCommands = new AtomicLong();
    AtomicLong optimizeCommands = new AtomicLong();
    AtomicLong rollbackCommands = new AtomicLong();
    AtomicLong numDocsPending = new AtomicLong();
    AtomicLong numErrors = new AtomicLong();
    AtomicLong numErrorsCumulative = new AtomicLong();

    
    public SolandraIndexWriter(SolrCore core) {
        super(core);
         
        indexManager = new RedisIndexManager(CassandraUtils.service);
        
        try {
            
            writer = new lucandra.IndexWriter("");
            
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public int addDoc(AddUpdateCommand cmd) throws IOException {

        addCommands.incrementAndGet();
        addCommandsCumulative.incrementAndGet();
        int rc = -1;

        
        // if there is no ID field, use allowDups
        if (idField == null) {
            cmd.allowDups = true;
            cmd.overwriteCommitted = false;
            cmd.overwritePending = false;
        }

        try {

            //Term updateTerm = null;

            int docId =  indexManager.getCurrentDocId(core.getName());
                
            int shard = AbstractIndexManager.getShardFromDocId(docId);
            
            String indexName = core.getName()+"~"+shard;
            writer.setIndexName(indexName);
            
          /*  if (cmd.overwriteCommitted || cmd.overwritePending) {
                if (cmd.indexedId == null) {
                    cmd.indexedId = getIndexedId(cmd.doc);
                }
                Term idTerm = this.idTerm.createTerm(cmd.indexedId);
                boolean del = false;
                if (cmd.updateTerm == null) {
                    updateTerm = idTerm;
                } else {
                    del = true;
                    updateTerm = cmd.updateTerm;
                }

                
                
                writer.updateDocument(updateTerm, cmd.getLuceneDocument(schema), schema.getAnalyzer(), docId);
                if (del) { // ensure id remains unique
                    BooleanQuery bq = new BooleanQuery();
                    bq.add(new BooleanClause(new TermQuery(updateTerm), Occur.MUST_NOT));
                    bq.add(new BooleanClause(new TermQuery(idTerm), Occur.MUST));
                    writer.deleteDocuments(bq);
                }
            } else {*/
                // allow duplicates
                writer.addDocument(cmd.getLuceneDocument(schema), schema.getAnalyzer(), docId);
            //}

            rc = 1;
        } finally {
            if (rc != 1) {
                numErrors.incrementAndGet();
                numErrorsCumulative.incrementAndGet();
            }
        }

        return rc;

    }

    
    public void close() throws IOException {
        // hehe
    }

    
    public void commit(CommitUpdateCommand cmd) throws IOException {
        // hehe
    }

    
    public void delete(DeleteUpdateCommand cmd) throws IOException {
        deleteByIdCommands.incrementAndGet();
        deleteByIdCommandsCumulative.incrementAndGet();

        if (!cmd.fromPending && !cmd.fromCommitted) {
            numErrors.incrementAndGet();
            numErrorsCumulative.incrementAndGet();
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "meaningless command: " + cmd);
        }
        if (!cmd.fromPending || !cmd.fromCommitted) {
            numErrors.incrementAndGet();
            numErrorsCumulative.incrementAndGet();
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "operation not supported" + cmd);
        }

        writer.deleteDocuments(idTerm.createTerm(idFieldType.toInternal(cmd.id)));
    }

    
    public void deleteByQuery(DeleteUpdateCommand cmd) throws IOException {
        deleteByQueryCommands.incrementAndGet();
        deleteByQueryCommandsCumulative.incrementAndGet();

        if (!cmd.fromPending && !cmd.fromCommitted) {
            numErrors.incrementAndGet();
            numErrorsCumulative.incrementAndGet();
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "meaningless command: " + cmd);
        }
        if (!cmd.fromPending || !cmd.fromCommitted) {
            numErrors.incrementAndGet();
            numErrorsCumulative.incrementAndGet();
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "operation not supported" + cmd);
        }

        boolean madeIt = false;
        boolean delAll = false;
        try {
            Query q = QueryParsing.parseQuery(cmd.query, schema);
            delAll = MatchAllDocsQuery.class == q.getClass();

            if (delAll) {

                throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "can't delete all: " + cmd);
            } else {
                writer.deleteDocuments(q);
            }

            madeIt = true;

        } finally {
            if (!madeIt) {
                numErrors.incrementAndGet();
                numErrorsCumulative.incrementAndGet();
            }
        }

    }

    
    public int mergeIndexes(MergeIndexesCommand cmd) throws IOException {
        // haha
        return 0;
    }

    
    public void rollback(RollbackUpdateCommand cmd) throws IOException {
        // TODO Auto-generated method stub

    }

    
    public Category getCategory() {
        return Category.UPDATEHANDLER;
    }

    
    public String getDescription() {
        return "Update handler for Solandra";
    }

    
    public URL[] getDocs() {
        // TODO Auto-generated method stub
        return null;
    }

    
    public String getName() {
        return SolandraIndexWriter.class.getName();
    }

    
    public String getSource() {
        // TODO Auto-generated method stub
        return null;
    }

    
    public String getSourceId() {
        // TODO Auto-generated method stub
        return null;
    }

    
    @SuppressWarnings("unchecked")
    public NamedList getStatistics() {
        NamedList lst = new SimpleOrderedMap();

        lst.add("rollbacks", rollbackCommands.get());
        lst.add("adds", addCommands.get());
        lst.add("deletesById", deleteByIdCommands.get());
        lst.add("deletesByQuery", deleteByQueryCommands.get());
        lst.add("errors", numErrors.get());
        lst.add("cumulative_adds", addCommandsCumulative.get());
        lst.add("cumulative_deletesById", deleteByIdCommandsCumulative.get());
        lst.add("cumulative_deletesByQuery", deleteByQueryCommandsCumulative.get());
        lst.add("cumulative_errors", numErrorsCumulative.get());
        return lst;
    }

   
    public String getVersion() {
        return core.getVersion();
    }

}
