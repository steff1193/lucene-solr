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

package org.apache.solr.client.solrj;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.ParameterizedType;
import java.util.Collection;

import org.apache.solr.client.solrj.response.SolrResponseBase;
import org.apache.solr.common.exceptions.PartialErrors;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.security.AuthCredentials;

/**
 * 
 *
 * @since solr 1.3
 */
public abstract class SolrRequest<RESPONSE extends SolrResponseBase> implements Serializable
{
  public enum METHOD {
    GET,
    POST
  };

  private METHOD method = METHOD.GET;
  private String path = null;

  private ResponseParser responseParser;
  private StreamingResponseCallback callback;
  private AuthCredentials authCredentials;
  private boolean preemptiveAuthentication = true;
  
  //---------------------------------------------------------
  //---------------------------------------------------------

  public SolrRequest( METHOD m, String path )
  {
    this.method = m;
    this.path = path;
  }

  //---------------------------------------------------------
  //---------------------------------------------------------

  public METHOD getMethod() {
    return method;
  }
  public void setMethod(METHOD method) {
    this.method = method;
  }

  public String getPath() {
    return path;
  }
  public void setPath(String path) {
    this.path = path;
  }

  /**
   *
   * @return The {@link org.apache.solr.client.solrj.ResponseParser}
   */
  public ResponseParser getResponseParser() {
    return responseParser;
  }

  /**
   * Optionally specify how the Response should be parsed.  Not all server implementations require a ResponseParser
   * to be specified.
   * @param responseParser The {@link org.apache.solr.client.solrj.ResponseParser}
   */
  public void setResponseParser(ResponseParser responseParser) {
    this.responseParser = responseParser;
  }

  public StreamingResponseCallback getStreamingResponseCallback() {
    return callback;
  }

  public void setStreamingResponseCallback(StreamingResponseCallback callback) {
    this.callback = callback;
  }
  
  public AuthCredentials getAuthCredentials() {
    return authCredentials;
  }

  public void setAuthCredentials(AuthCredentials authCredentials) {
    this.authCredentials = authCredentials;
  }

  public boolean getPreemptiveAuthentication() {
    return preemptiveAuthentication;
  }

  public void setPreemptiveAuthentication(boolean preemptiveAuthentication) {
    this.preemptiveAuthentication = preemptiveAuthentication;
  }

  public abstract SolrParams getParams();
  public abstract Collection<ContentStream> getContentStreams() throws IOException;
  
  public RESPONSE getResponse(NamedList<Object> res , SolrServer solrServer) {
    Class<?> directSolrRequestSubclass = getClass();
    while (!directSolrRequestSubclass.getSuperclass().equals(SolrRequest.class)) directSolrRequestSubclass = directSolrRequestSubclass.getSuperclass();
    Class<? extends SolrResponseBase> responseClass = (Class<? extends SolrResponseBase>)((ParameterizedType)directSolrRequestSubclass.getGenericSuperclass()).getActualTypeArguments()[0];

    try {
      return (RESPONSE)responseClass.getConstructor().newInstance();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  
  @SuppressWarnings("unchecked")
  public RESPONSE process( SolrServer server ) throws SolrServerException, IOException
  {
    
		PartialErrors peToBeThrown = null;
    long startTime = System.currentTimeMillis();
    NamedList<Object> genericResponse;
    try {
    	genericResponse = server.request(this);
    } catch (PartialErrors pe) {
    	genericResponse = pe.getPayload();
    	peToBeThrown = pe;
    }
    RESPONSE res = getResponse(genericResponse, server);
    res.setResponse(genericResponse);
    res.setElapsedTime( System.currentTimeMillis()-startTime );
    
    if (peToBeThrown != null) {
    	peToBeThrown.setSpecializedResponse(res);
    	throw peToBeThrown;
    }
    
    return (RESPONSE) res;
  }
}
