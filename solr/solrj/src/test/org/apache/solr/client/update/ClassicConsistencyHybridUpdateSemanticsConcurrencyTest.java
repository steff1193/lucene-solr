/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.client.update;

import java.io.IOException;
import java.util.Random;

import org.apache.solr.SolrJettyTestBase;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.exceptions.update.DocumentAlreadyExists;
import org.apache.solr.common.exceptions.update.DocumentDoesNotExist;
import org.apache.solr.common.exceptions.update.VersionConflict;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.util.ExternalPaths;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ClassicConsistencyHybridUpdateSemanticsConcurrencyTest extends
    SolrJettyTestBase {
  @BeforeClass
  public static void beforeTest() throws Exception {
    // Not necessary to set solr.semantics.mode to anything, because classic-consistency-hybrid is default
    System.setProperty("solr.semantics.mode", "classic-consistency-hybrid");
    createJetty(ExternalPaths.EXAMPLE_HOME, null, null);
  }
  
  @Before
  public void doBefore() throws IOException, SolrServerException {
    SolrServer solrServer = getSolrServer();
    solrServer.deleteByQuery("*:*");
    solrServer.commit();
  }
  
  private SolrDocument realtimeGetSMSDocById(final SolrServer solrServer) throws Exception
  {
    final SolrQuery query = new SolrQuery();
    query.set(CommonParams.QT, "/get");
    query.set("id", "A");
    
    QueryRequest req = new QueryRequest( query );
    QueryResponse rsp = req.process(solrServer);
    SolrDocument result = (SolrDocument)rsp.getResponse().get("doc");
    return result;
  }

  
  private class ConcurrentUpdater implements Runnable {
    private SolrServer solrServer;
    private boolean timedOut;
    
    public ConcurrentUpdater(SolrServer solrServer) {
      super();
      this.solrServer = solrServer;
      this.timedOut = false;
    }

    @Override
    public void run() {
      try {
        long TIMEOUT_MS = 5 * 60 * 1000;
        boolean documentExists = false;
        boolean success = false;
        long start = System.currentTimeMillis();
        while (!success && ((System.currentTimeMillis() - start) < TIMEOUT_MS)) {
          try {
            if (!documentExists) {
              createDocument();
            } else {
              if (!updateDocument()) {
                documentExists = false;
                continue;
              }
            }
            success = true;
          } catch (DocumentAlreadyExists e) {
            documentExists = true;
          } catch (DocumentDoesNotExist e2) {
            documentExists = false;
          } catch (VersionConflict e2) {
            documentExists = true;
          }
        }
        if (!success) timedOut = true;
      } catch (Exception e) {
        System.out.println("******** UNEXPECTED EXCEPTION **************");
        e.printStackTrace(System.out);
      }
    }
    
    private void createDocument() throws Exception {
      SolrInputDocument idoc = new SolrInputDocument();
      idoc.addField("id", "A");
      idoc.addField("popularity", 1);
      idoc.addField(SolrInputDocument.VERSION_FIELD, -1);
      // _version_ explicitly set to -1. Therefore this is a create
      solrServer.add(idoc);
    }
    
    private boolean updateDocument() throws Exception {
      SolrDocument doc = realtimeGetSMSDocById(solrServer);
      if (doc != null) {
        SolrInputDocument idoc = ClientUtils.toSolrInputDocument(doc);
        int oldVal = (Integer)idoc.getFieldValue("popularity");
        int newVal = oldVal + 1;
        Thread.sleep(random().nextInt(150));
        idoc.removeField("popularity");
        idoc.addField("popularity", newVal);
        // The document existed and will therefore have a _version_ > 0. Therefore this is an update
        solrServer.add(idoc);
        return true;
      }
      return false;
    }
    
  }
  @Test
  public void crazyConcurrencyTest() throws Exception {
    int CONCURRENT_THREADS = 50;
    ConcurrentUpdater[] updaters = new ConcurrentUpdater[CONCURRENT_THREADS];
    Thread[] threads = new Thread[CONCURRENT_THREADS];
    for (int i = 0; i < CONCURRENT_THREADS; i++) {
      updaters[i] = new ConcurrentUpdater(createNewSolrServer());
      threads[i] = new Thread(updaters[i]);
    }
    for (int i = 0; i < CONCURRENT_THREADS; i++) {
      threads[i].start();
    }
    for (int i = 0; i < CONCURRENT_THREADS; i++) {
      threads[i].join();
    }
    int timeouts = 0;
    for (int i = 0; i < CONCURRENT_THREADS; i++) {
      if (updaters[i].timedOut) timeouts++;
    }
    assertEquals(0, timeouts);
    SolrDocument doc = realtimeGetSMSDocById(createNewSolrServer());
    assertEquals(CONCURRENT_THREADS, ((Integer)doc.getFieldValue("popularity")).intValue());
  }

}
