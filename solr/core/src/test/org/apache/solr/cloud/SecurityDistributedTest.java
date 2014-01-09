package org.apache.solr.cloud;

import static org.apache.solr.client.solrj.embedded.JettySolrRunner.ALL_PASSWORD;
import static org.apache.solr.client.solrj.embedded.JettySolrRunner.ALL_USERNAME;
import static org.apache.solr.client.solrj.embedded.JettySolrRunner.SEARCH_CREDENTIALS;
import static org.apache.solr.client.solrj.embedded.JettySolrRunner.SEARCH_PASSWORD;
import static org.apache.solr.client.solrj.embedded.JettySolrRunner.SEARCH_USERNAME;
import static org.apache.solr.client.solrj.embedded.JettySolrRunner.UPDATE_CREDENTIALS;
import static org.apache.solr.client.solrj.embedded.JettySolrRunner.UPDATE_PASSWORD;
import static org.apache.solr.client.solrj.embedded.JettySolrRunner.UPDATE_USERNAME;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.SolrRequest.METHOD;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.cloud.InterSolrNodeAuthCredentialsFactoryTestingHelper.SubRequestFactoryWrapper;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.exceptions.PartialErrors;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.security.AuthCredentials;
import org.apache.solr.security.AuthCredentials.AbstractAuthMethod;
import org.apache.solr.security.InterSolrNodeAuthCredentialsFactory.AuthCredentialsSource;

/*
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

/*
 * Focus of SecurityDistributedTest is to test that access is denied on unauthenticated and/or
 * unauthorized credentials. Since we run with correct credentials across all other tests extending
 * BaseDistributedSearchTestCase the access-on-correct-credentials should be very well
 * tested elsewhere. 
 */
@Slow
public class SecurityDistributedTest extends AbstractFullDistribZkTestBase {
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    System.setProperty(HttpClientUtil.HTTP_CLIENTS_MUST_ADAPT_TO_CREDENTIALS_CHANGES, "true");
  }

  @Override
  public void tearDown() throws Exception {
    System.clearProperty(HttpClientUtil.HTTP_CLIENTS_MUST_ADAPT_TO_CREDENTIALS_CHANGES);
    super.tearDown();
  }


  // Ahhh, when will we have real closures in java
  private void doAndAssertSolrExeption(int expectedStatusCode, Callable<Object> toDo) throws Exception {
    doAndAssertSolrExeption(expectedStatusCode, null, toDo);
  }
  private void doAndAssertSolrExeption(int expectedStatusCode, List<Integer> expectedPartialErrorStatusCodes, Callable<Object> toDo) throws Exception {
    try {
      toDo.call();
      fail(((expectedPartialErrorStatusCodes == null)?SolrException.class.getName():PartialErrors.class.getName()) + " with status-code " + expectedStatusCode + " expected." 
          + ((expectedPartialErrorStatusCodes != null)?(" Expected to have " + expectedPartialErrorStatusCodes.size() + " partialerrors with the following status-codes " + expectedPartialErrorStatusCodes):""));
    } catch (Exception e) {
      if (expectedStatusCode > 0) assertTrue(containsStatusCodeInCausesChain(e, expectedStatusCode));
      if (expectedPartialErrorStatusCodes != null) {
        assertEquals(PartialErrors.class.getName(), e.getClass().getName());
        Map<String, SolrException> partialErrors = ((PartialErrors)e).getSpecializedResponse().getPartialErrors();
        assertEquals(expectedPartialErrorStatusCodes.size(), partialErrors.size());
        int i = 0;
        for (SolrException partialError : partialErrors.values()) {
          assertEquals(expectedPartialErrorStatusCodes.get(i++).intValue(), partialError.code());
        }
      }
    }
  }
  
  private boolean containsStatusCodeInCausesChain(Throwable e, int statusCode) {
    return 
      (e instanceof SolrException && ((SolrException)e).code() == statusCode) ||
      (e.getCause() != null && containsStatusCodeInCausesChain(e.getCause(), statusCode));
  }
  
  private Set<AbstractAuthMethod> oldInternalAuthMethods;

  private void modifyAllInternalAuthCrendtials(AuthCredentials newAuthCredentials) {
    AuthCredentials internalAuthCredentials = AuthCredentialsSource.useInternalAuthCredentials().getAuthCredentials();
    oldInternalAuthMethods = internalAuthCredentials.getAuthMethods();
    internalAuthCredentials.setAuthMethods(newAuthCredentials.getAuthMethods());
  }
  
  private void revertModificationAllInternalAuthCrendtials() {
    AuthCredentials internalAuthCredentials = AuthCredentialsSource.useInternalAuthCredentials().getAuthCredentials();
    if (oldInternalAuthMethods != null) {
      internalAuthCredentials.setAuthMethods(oldInternalAuthMethods);
    }
    oldInternalAuthMethods = null;
  }
  
  private void modifyInterSolrNodeAuthCredentialsFactoryForSubRequests(final AuthCredentials newAuthCredentials) {
    InterSolrNodeAuthCredentialsFactoryTestingHelper.pushSubRequestFactoryWrapper(
        new SubRequestFactoryWrapper() {

          @Override
          public AuthCredentials getFromOuterRequest(SolrQueryRequest outerRequest) {
            return newAuthCredentials;
          }
          
        });
  }
  
  private void revertModificationInterSolrNodeAuthCredentialsFactoryForSubRequests() {
    InterSolrNodeAuthCredentialsFactoryTestingHelper.popSubRequestFactoryWrapper();
  }

  
  @Override
  public void doTest() throws Exception {
    
    ZkStateReader zkStateReader = cloudClient.getZkStateReader();
    // make sure we have leaders for each shard
    for (int j = 1; j < sliceCount; j++) {
      zkStateReader.getLeaderRetry(DEFAULT_COLLECTION, "shard" + j, 10000);
    }
    
    waitForRecoveriesToFinish(false);
    
    del("*:*");

    // Test unauthenticated delete
    doAndAssertSolrExeption(401, new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        controlClient.deleteByQuery("*:*", -1, AuthCredentials.createBasicAuthCredentials(UPDATE_USERNAME, UPDATE_PASSWORD + "wrong"));
        return null;
      }
    });

    // Test authenticated but unauthorized delete
    doAndAssertSolrExeption(403, new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        controlClient.deleteByQuery("*:*", -1, SEARCH_CREDENTIALS);
        return null;
      }
    });
    
    // Test unauthenticated index
    doAndAssertSolrExeption(401, new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        controlClient.add(new SolrInputDocument(), -1, AuthCredentials.createBasicAuthCredentials(UPDATE_USERNAME, UPDATE_PASSWORD + "wrong"));
        return null;
      }
    });

    // Test authenticated but unauthorized index
    doAndAssertSolrExeption(403, new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        controlClient.add(new SolrInputDocument(), -1, SEARCH_CREDENTIALS);
        return null;
      }
    });

    // Test unauthenticated search
    doAndAssertSolrExeption(401, new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        final ModifiableSolrParams params = new ModifiableSolrParams();
        params.add("q", "*:*");
        params.set("distrib", "false");
        controlClient.query(params, METHOD.GET, AuthCredentials.createBasicAuthCredentials(SEARCH_USERNAME, SEARCH_PASSWORD + "wrong"));
        return null;
      }
    });

    // Test authenticated but unauthorizaed search
    doAndAssertSolrExeption(403, new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        final ModifiableSolrParams params = new ModifiableSolrParams();
        params.add("q", "*:*");
        params.set("distrib", "false");
        controlClient.query(params, METHOD.GET, UPDATE_CREDENTIALS);
        return null;
      }
    });

    // Test unauthenticated get
    doAndAssertSolrExeption(401, new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        final ModifiableSolrParams params = new ModifiableSolrParams();
        params.add("qt","/get");
        params.set("id",Integer.toString(100));
        controlClient.query(params, METHOD.GET, AuthCredentials.createBasicAuthCredentials(SEARCH_USERNAME, SEARCH_PASSWORD + "wrong"));
        return null;
      }
    });

    // Test authenticated but unauthorizaed get
    doAndAssertSolrExeption(403, new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        final ModifiableSolrParams params = new ModifiableSolrParams();
        params.add("qt","/get");
        params.set("id",Integer.toString(100));
        controlClient.query(params, METHOD.GET, UPDATE_CREDENTIALS);
        return null;
      }
    });
    
    // Check that you can create a collection
    createCollection("security_new_collection1", shardCount);
    List<Integer> expectedShardsReplica = new ArrayList<Integer>();
    expectedShardsReplica.add(shardCount);
    expectedShardsReplica.add(0);
    checkForCollection("security_new_collection1", expectedShardsReplica, null);
    
    // Test unauthenticated user in sub-requests from Collection API to Core Admin API (creating the individual shards)
    modifyAllInternalAuthCrendtials(AuthCredentials.createBasicAuthCredentials(ALL_USERNAME, ALL_PASSWORD + "wrong"));
    try {
      doAndAssertSolrExeption(ErrorCode.PRECONDITION_FAILED.code, 
          Arrays.asList(new Integer[]{ErrorCode.UNAUTHORIZED.code, ErrorCode.UNAUTHORIZED.code, ErrorCode.UNAUTHORIZED.code, ErrorCode.UNAUTHORIZED.code}), 
          new Callable<Object>() {
            @Override
            public Object call() throws Exception {
              createCollection("security_new_collection2", shardCount);
              return null;
            }
          });
    } finally {
      revertModificationAllInternalAuthCrendtials();
    }

    // Test authenticated but authorized user in sub-requests from Collection API to Core Admin API (creating the individual shards)
    modifyAllInternalAuthCrendtials(SEARCH_CREDENTIALS);
    try {
      doAndAssertSolrExeption(ErrorCode.PRECONDITION_FAILED.code, 
          Arrays.asList(new Integer[]{ErrorCode.FORBIDDEN.code, ErrorCode.FORBIDDEN.code, ErrorCode.FORBIDDEN.code, ErrorCode.FORBIDDEN.code}),
          new Callable<Object>() {
            @Override
            public Object call() throws Exception {
              createCollection("security_new_collection3", shardCount);
              return null;
            }
          });
    } finally {
      revertModificationAllInternalAuthCrendtials();
    }

    // Test authenticated but unauthorized user in recovery-sub-requests (assuming authenticated will work the same way)
    modifyAllInternalAuthCrendtials(AuthCredentials.createBasicAuthCredentials(ALL_USERNAME, ALL_PASSWORD + "wrong"));
    try {
      // shutdown replica
      JettySolrRunner replica = chaosMonkey.stopShard("shard1", 1).jetty;
      
      // index a few docs (enough to be "sure" we hit the downed shard)
      for (int i = 0; i < 10; i++) {
        indexr("id", i);
      }

      Thread.sleep(atLeast(2000));
      
      // bring replica up
      replica.start();
      
      boolean sawProblemRecovering = false;
      try {
        // Normally with functioning recovery it takes a few secs to recover, so waiting 15 secs to see 
        // that it is not successful must be enough
        waitForRecoveriesToFinish(DEFAULT_COLLECTION, zkStateReader, false, true, 15, true);
      } catch (AssertionError e) {
        assertEquals("There are still nodes recoverying - waited for 15 seconds", e.getMessage());
        sawProblemRecovering = true;
      }
      if (!sawProblemRecovering) {
        fail("Expected that recovery would be unsuccessful with wrong internal credentials");
      }
    } finally {
      revertModificationAllInternalAuthCrendtials();
    }
    
    // Test unauthenticated delete on the internal solr-node-to-solr-node request, but authenticated and authorized on request from "outside"
    modifyInterSolrNodeAuthCredentialsFactoryForSubRequests(AuthCredentials.createBasicAuthCredentials(UPDATE_USERNAME, UPDATE_PASSWORD + "wrong"));
    try {
      // Non-distributed should work, because there is no sub-requests sent
      controlClient.deleteByQuery("*:*", -1, UPDATE_CREDENTIALS);
      doAndAssertSolrExeption(ErrorCode.PRECONDITION_FAILED.code, 
          Arrays.asList(new Integer[]{ErrorCode.UNAUTHORIZED.code /* two? , ErrorCode.UNAUTHORIZED.code*/}),
          new Callable<Object>() {
            @Override
            public Object call() throws Exception {
              cloudClient.deleteByQuery("*:*", -1, UPDATE_CREDENTIALS);
              return null;
            }
          });
    } finally {
      revertModificationInterSolrNodeAuthCredentialsFactoryForSubRequests();
    }
    
    // Test authenticated but unauthorizaed delete on the internal solr-node-to-solr-node request, but authenticated and authorized on request from "outside"
    modifyInterSolrNodeAuthCredentialsFactoryForSubRequests(SEARCH_CREDENTIALS);
    try {
      // Non-distributed should work, because there is no sub-requests sent
      controlClient.deleteByQuery("*:*", -1, UPDATE_CREDENTIALS);
      doAndAssertSolrExeption(ErrorCode.PRECONDITION_FAILED.code, 
          Arrays.asList(new Integer[]{ErrorCode.FORBIDDEN.code /* two? , ErrorCode.FORBIDDEN.code*/}), new Callable<Object>() {
            @Override
            public Object call() throws Exception {
              cloudClient.deleteByQuery("*:*", -1, UPDATE_CREDENTIALS);
              return null;
            }
          });
    } finally {
      revertModificationInterSolrNodeAuthCredentialsFactoryForSubRequests();
    }
    
    final SolrInputDocument doc = new SolrInputDocument();
    // Test unauthenticated index on the internal solr-node-to-solr-node request, but authenticated and authorized on request from "outside"
    modifyInterSolrNodeAuthCredentialsFactoryForSubRequests(AuthCredentials.createBasicAuthCredentials(UPDATE_USERNAME, UPDATE_PASSWORD + "wrong"));
    try {
      doc.addField(id, 100);
      // Non-distributed should work, because there is no sub-requests sent
      controlClient.add(doc, -1, UPDATE_CREDENTIALS);
      doAndAssertSolrExeption(401, new Callable<Object>() {
        @Override
        public Object call() throws Exception {
          // Index the doc against to concrete nodes - they cannot both run the leader-replica of the shard where the doc belongs and
          // will therefore have to forward to leader in a sub-reguest
          clients.get(0).add(doc, -1, UPDATE_CREDENTIALS);
          clients.get(1).add(doc, -1, UPDATE_CREDENTIALS);
          return null;
        }
      });
    } finally {
      revertModificationInterSolrNodeAuthCredentialsFactoryForSubRequests();
    }

    // Test authenticated but unauthorizaed index on the internal solr-node-to-solr-node request, but authenticated and authorized on request from "outside"
    modifyInterSolrNodeAuthCredentialsFactoryForSubRequests(SEARCH_CREDENTIALS);
    try {
      // Non-distributed should work, because there is no sub-requests sent
      controlClient.add(doc, -1, UPDATE_CREDENTIALS);
      doAndAssertSolrExeption(403, new Callable<Object>() {
        @Override
        public Object call() throws Exception {
          // Index the doc against to concrete nodes - they cannot both run the leader-replica of the shard where the doc belongs and
          // will therefore have to forward to leader in a sub-reguest
          clients.get(0).add(doc, -1, UPDATE_CREDENTIALS);
          clients.get(1).add(doc, -1, UPDATE_CREDENTIALS);
          return null;
        }
      });
    } finally {
      revertModificationInterSolrNodeAuthCredentialsFactoryForSubRequests();
    }

    final ModifiableSolrParams selectParams = new ModifiableSolrParams();
    
    // Test unauthenticated search on the internal solr-node-to-solr-node request, but authenticated and authorized on request from "outside"
    modifyInterSolrNodeAuthCredentialsFactoryForSubRequests(AuthCredentials.createBasicAuthCredentials(SEARCH_USERNAME, SEARCH_PASSWORD + "wrong"));
    try {
      // Non-distributed should work, because there is no sub-requests sent
      selectParams.add("q", "*:*");
      selectParams.set("distrib", "false");
      controlClient.query(selectParams, METHOD.GET, SEARCH_CREDENTIALS);
      doAndAssertSolrExeption(401, new Callable<Object>() {
        @Override
        public Object call() throws Exception {
          selectParams.set("distrib", "true");
          cloudClient.query(selectParams, METHOD.GET, SEARCH_CREDENTIALS);
          return null;
        }
      });
    } finally {
      revertModificationInterSolrNodeAuthCredentialsFactoryForSubRequests();
    }
 
    // Test authenticated but unauthorizaed search on the internal solr-node-to-solr-node request, but authenticated and authorized on request from "outside"
    modifyInterSolrNodeAuthCredentialsFactoryForSubRequests(UPDATE_CREDENTIALS);
    try {
      // Non-distributed should work, because there is no sub-requests sent
      selectParams.set("distrib", "false");
      controlClient.query(selectParams, METHOD.GET, SEARCH_CREDENTIALS);
      // TODO It ought to have been 403 below instead of -1, but things are just crappy with respect to 403 handling around the code
      doAndAssertSolrExeption(-1 /*403*/, new Callable<Object>() {
        @Override
        public Object call() throws Exception {
          selectParams.set("distrib", "true");
          cloudClient.query(selectParams, METHOD.GET, SEARCH_CREDENTIALS);
          return null;
        }
      });
    } finally {
      revertModificationInterSolrNodeAuthCredentialsFactoryForSubRequests();
    }

    final ModifiableSolrParams getParams = new ModifiableSolrParams();
    // Test unauthenticated get on the internal solr-node-to-solr-node request, but authenticated and authorized on request from "outside"
    modifyInterSolrNodeAuthCredentialsFactoryForSubRequests(AuthCredentials.createBasicAuthCredentials(SEARCH_USERNAME, SEARCH_PASSWORD + "wrong"));
    try {
      // Non-distributed should work, because there is no sub-requests sent
      getParams.add("qt","/get");
      getParams.set("id",Integer.toString(100));
      /* TODO Seems like the single control-node is sending requests to itself in order to handle get!?
      controlClient.query(params, METHOD.GET, SEARCH_CREDENTIALS);*/
      doAndAssertSolrExeption(401, new Callable<Object>() {
        @Override
        public Object call() throws Exception {
          clients.get(0).query(getParams, METHOD.GET, SEARCH_CREDENTIALS);
          clients.get(1).query(getParams, METHOD.GET, SEARCH_CREDENTIALS);
          return null;
        }
      });
    } finally {
      revertModificationInterSolrNodeAuthCredentialsFactoryForSubRequests();
    }
 
    // Test authenticated but unauthorizaed get on the internal solr-node-to-solr-node request, but authenticated and authorized on request from "outside"
    modifyInterSolrNodeAuthCredentialsFactoryForSubRequests(UPDATE_CREDENTIALS);
    try {
      // Non-distributed should work, because there is no sub-requests sent
      /* TODO Seems like the single control-node is sending requests to itself in order to handle get!?
      controlClient.query(params, METHOD.GET, SEARCH_CREDENTIALS);*/
      // TODO It ought to have been 403 below instead of -1, but things are just crappy with respect to 403 handling around the code
      doAndAssertSolrExeption(-1 /*403*/, new Callable<Object>() {
        @Override
        public Object call() throws Exception {
          clients.get(0).query(getParams, METHOD.GET, SEARCH_CREDENTIALS);
          clients.get(1).query(getParams, METHOD.GET, SEARCH_CREDENTIALS);
          return null;
        }
      });
    } finally {
      revertModificationInterSolrNodeAuthCredentialsFactoryForSubRequests();
    }
    
    // Test non-preemptive POST request with authentication - if you are not careful how you provide the body for POST requests
    // which require credentials but where you do not provide them preemptively, you will end up not being able to re-send the request
    // after the server responded with 401 to the first request
    UpdateRequest req = new UpdateRequest();
    req.add(doc);
    req.setAuthCredentials(UPDATE_CREDENTIALS);
    req.setPreemptiveAuthentication(false);
    req.process(clients.get(0));
    
  }
  
}
