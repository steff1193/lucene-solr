package org.apache.solr.security;

import java.util.Collections;
import java.util.HashSet;
import java.util.Observable;
import java.util.Set;

import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrServer;

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

public class AuthCredentials extends Observable {

  public interface AbstractAuthMethod {}
  
  // For now only basic http auth supported
  public static class BasicHttpAuth implements AbstractAuthMethod {

    private String username;
    private String password;

    public BasicHttpAuth(String username, String password) {
      this.username = username;
      this.password = password;
    }

    public String getUsername() {
      return username;
    }

    public String getPassword() {
      return password;
    }
    
    public String toString() {
      return new StringBuilder("BasicHttpAuth - username: ").append(username).append(", password: ").append(password).toString();
    }
    
  }

  protected Set<AbstractAuthMethod> authMethods;

  public AuthCredentials(Set<AbstractAuthMethod> authMethods) {
    setAuthMethods(authMethods);
  }

  public Set<AbstractAuthMethod> getAuthMethods() {
    return authMethods;
  }
  
  public void setAuthMethods(Set<AbstractAuthMethod> authMethods) {
    if (authMethods == null) {
      authMethods = new HashSet<AbstractAuthMethod>();
    }
    // TODO we ought to test if authMethods is already unmodifiable and not wrap it if it is, but I hope/guess
    // Collections.unmodifiableSet will do that internally - I found no way to test if a Set is unmodifiable
    this.authMethods = Collections.unmodifiableSet(authMethods);
    this.setChanged();
    this.notifyObservers();
  }
  
  public void applyToHttpClient(HttpClient httpClient) {
    if (httpClient == null) return;
    if (httpClient instanceof DefaultHttpClient) {
      applyToCredentialsProvider(((DefaultHttpClient)httpClient).getCredentialsProvider());
    }
  }

  public void applyToCredentialsProvider(CredentialsProvider credentialsProvider) {
    if (credentialsProvider == null) return;
    clearCredentials(credentialsProvider);
    for (AbstractAuthMethod abstractAuthMethod : getAuthMethods()) {
      if (abstractAuthMethod instanceof BasicHttpAuth) {
        BasicHttpAuth basicAuth = (BasicHttpAuth)abstractAuthMethod;
          credentialsProvider.setCredentials(AuthScope.ANY,
              new UsernamePasswordCredentials(basicAuth.getUsername(), basicAuth.getPassword()));
      }
    }
  }

  public static void clearCredentials(CredentialsProvider credentialsProvider) {
    if (credentialsProvider == null) return;
    credentialsProvider.clear();
  }
  
  public String toString() {
    StringBuilder sb = new StringBuilder("AuthCredentials\n");
    for (AbstractAuthMethod abstractAuthMethod : getAuthMethods()) {
      sb.append("  ").append(abstractAuthMethod.toString()).append("\n");
    }
    return sb.toString();
  }


  public static AuthCredentials createBasicAuthCredentials(String username, String password) {
    Set<AbstractAuthMethod> authMethods = new HashSet<AbstractAuthMethod>();
    authMethods.add(new BasicHttpAuth(username, password));
    return new AuthCredentials(authMethods);
  }
  
}
