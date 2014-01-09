package org.apache.solr.security;

import java.util.HashSet;
import java.util.Set;

import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.security.AuthCredentials.AbstractAuthMethod;

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

public class InterSolrNodeAuthCredentialsFactory {
  
  public static final class AuthCredentialsSource {

    private AuthCredentials authCredentials;

    private AuthCredentialsSource(AuthCredentials authCredentials) {
      this.authCredentials = authCredentials;
    }
    
    public AuthCredentials getAuthCredentials() {
      return authCredentials;
    }

    private static AuthCredentialsSource internalAuthCredentials =
      new AuthCredentialsSource(InterSolrNodeAuthCredentialsFactory.getCurrentInternalRequestFactory().getInternalAuthCredentials());
    public static AuthCredentialsSource useInternalAuthCredentials() {
      return internalAuthCredentials;
    }

    public static AuthCredentialsSource useAuthCredentialsFromOuterRequest(SolrQueryRequest outerRequest) {
      return new AuthCredentialsSource(InterSolrNodeAuthCredentialsFactory.getCurrentSubRequestFactory().getFromOuterRequest(outerRequest));
    }

  } 

  // For requests issued as a direct reaction to a request coming from the "outside"
  
  private static SubRequestFactory currentSubRequestFactory = new DefaultSubRequestFactory();
  
  public static interface SubRequestFactory {
    AuthCredentials getFromOuterRequest(SolrQueryRequest outerRequest);
  }
  
  public static class DefaultSubRequestFactory implements SubRequestFactory {
    public AuthCredentials getFromOuterRequest(SolrQueryRequest outerRequest) {
      return outerRequest.getAuthCredentials();
    }
  }

  protected static SubRequestFactory getCurrentSubRequestFactory() {
    return currentSubRequestFactory;
  }

  public static void setCurrentSubRequestFactory(SubRequestFactory currentSubRequestFactory) {
    InterSolrNodeAuthCredentialsFactory.currentSubRequestFactory = currentSubRequestFactory;
  }
  
  // For requests issued on initiative from the solr-node itself 
  
  private static InternalRequestFactory currentInternalRequestFactory = new DefaultInternalRequestFactory();
  
  public static interface InternalRequestFactory {
    AuthCredentials getInternalAuthCredentials();
  }
  
  public static class DefaultInternalRequestFactory implements InternalRequestFactory {
    
    private boolean alreadyDone = false;
    private AuthCredentials internalAuthCredentials = null;

    @Override
    public synchronized AuthCredentials getInternalAuthCredentials() {
      if (!alreadyDone) {
        // TODO since internalAuthCredentials is something you use for "internal" requests against other Solr-nodes it should never
        // have different values for different Solr-nodes in the same cluster, and therefore the credentials ought to be specified
        // on a global level (e.g. in ZK) instead of on a "per node" level as solr.xml and VM-params are

        String internalBasicAuthUsername = System.getProperty("internalAuthCredentialsBasicAuthUsername");
        String internalBasicAuthPassword = System.getProperty("internalAuthCredentialsBasicAuthPassword");
        
        Set<AbstractAuthMethod> authMethods = new HashSet<AbstractAuthMethod>();
        if (internalBasicAuthUsername != null && internalBasicAuthPassword != null) {
          authMethods.add(new AuthCredentials.BasicHttpAuth(internalBasicAuthUsername, internalBasicAuthPassword));
        }
        if (internalAuthCredentials == null) {
          internalAuthCredentials = new AuthCredentials(authMethods);
        } else {
          // Not creating a new instance but replacing auth-methods in order to have the changes propagate
          // to objects already using and observing it
          internalAuthCredentials.setAuthMethods(authMethods);
        }
        alreadyDone = true;
      }
      return internalAuthCredentials;
    }
    
    public void recalculateNow() {
      alreadyDone = false;
      getInternalAuthCredentials();
    }
    
  }
  
  protected static InternalRequestFactory getCurrentInternalRequestFactory() {
    return currentInternalRequestFactory;
  }
  
  public static void setCurrentInternalRequestFactory(InternalRequestFactory currentInternalRequestFactory) {
    InterSolrNodeAuthCredentialsFactory.currentInternalRequestFactory = currentInternalRequestFactory;
  }
  
}
