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

package org.apache.solr.response;

import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.exceptions.PartialErrors;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.search.ReturnFields;
import org.apache.solr.search.SolrReturnFields;

/**
 * <code>SolrQueryResponse</code> is used by a query handler to return
 * the response to a query request.
 *
 * <p>
 * <a name="returnable_data" /><b>Note On Returnable Data...</b><br/>
 * A <code>SolrQueryResponse</code> may contain the following types of
 * Objects generated by the <code>SolrRequestHandler</code> that processed
 * the request.
 * </p>
 * <ul>
 *  <li>{@link String}</li>
 *  <li>{@link Integer}</li>
 *  <li>{@link Long}</li>
 *  <li>{@link Float}</li>
 *  <li>{@link Double}</li>
 *  <li>{@link Boolean}</li>
 *  <li>{@link Date}</li>
 *  <li>{@link org.apache.solr.search.DocList}</li>
 *  <li>{@link org.apache.solr.common.SolrDocument} (since 1.3)</li>
 *  <li>{@link org.apache.solr.common.SolrDocumentList} (since 1.3)</li>
 *  <li>{@link Map} containing any of the items in this list</li>
 *  <li>{@link NamedList} containing any of the items in this list</li>
 *  <li>{@link Collection} containing any of the items in this list</li>
 *  <li>Array containing any of the items in this list</li>
 *  <li>null</li>
 * </ul>
 * <p>
 * Other data types may be added to the SolrQueryResponse, but there is no guarantee
 * that QueryResponseWriters will be able to deal with unexpected types.
 * </p>
 *
 *
 * @since solr 0.9
 */
public class SolrQueryResponse {

  /**
   * Container for user defined values
   * @see #getValues
   * @see #add
   * @see #setAllValues
   * @see <a href="#returnable_data">Note on Returnable Data</a>
   */
  protected NamedList<Object> values = new SimpleOrderedMap<Object>();
  
  protected Map<String, SolrException> partsRefToPartialErrorMap = new HashMap<String, SolrException>();
  
/**
   * Container for storing information that should be logged by Solr before returning.
   */
  protected NamedList<Object> toLog = new SimpleOrderedMap<Object>();

  protected ReturnFields returnFields;

  // entire request failed if this is set...
  protected Exception err;

  /**
   * Should this response be tagged with HTTP caching headers?
   */
  protected boolean httpCaching=true;
  
  /***
   // another way of returning an error
  int errCode;
  String errMsg;
  ***/

  public SolrQueryResponse() {
  }
  
  
  /**
   * Gets data to be returned in this response
   * @see <a href="#returnable_data">Note on Returnable Data</a>
   */
  public NamedList getValues() { return values; }

  /**
   * Sets data to be returned in this response
   * @see <a href="#returnable_data">Note on Returnable Data</a>
   */
  public void setAllValues(NamedList<Object> nameValuePairs) {
    values=nameValuePairs;
  }

  /**
   * Sets the document field names of fields to return by default when
   * returning DocLists
   */
  public void setReturnFields(ReturnFields fields) {
    returnFields=fields;
  }

  /**
   * Gets the document field names of fields to return by default when
   * returning DocLists
   */
  public ReturnFields getReturnFields() {
    if( returnFields == null ) {
      returnFields = new SolrReturnFields(); // by default return everything
    }
    return returnFields;
  }


  /**
   * Appends a named value to the list of named values to be returned.
   * @param name  the name of the value - may be null if unnamed
   * @param val   the value to add - also may be null since null is a legal value
   * @see <a href="#returnable_data">Note on Returnable Data</a>
   */
  public void add(String name, Object val) {
    values.add(name,val);
  }

  /**
   * Causes an error to be returned instead of the results.
   */
  public void setException(Exception e) {
    err=e;
  }

  public void addHandledPart(String partRef) {
    SolrResponse.addHandledPart(values, partRef);
  }
    
  /**
   * Returns an Exception if there was a fatal error in processing the request.
   * Returns null if the request succeeded.
   */
  public Exception getException() {
  	if (err == null) {
      @SuppressWarnings("unchecked")
      // If only on part of request handled and it resulted in error, throw corresponding exception for convenience
      SolrException singlePartialError;
      List<String> handledPartsRef = SolrResponse.getHandledPartsRef(getValues());
      if ((handledPartsRef != null) && handledPartsRef.size() == 1 && (singlePartialError = SolrResponse.getPartialError(partsRefToPartialErrorMap, getValues(), handledPartsRef.iterator().next())) != null) {
      	SolrResponse.removeAllPartsRef(partsRefToPartialErrorMap, getValues());
      	err = singlePartialError;
      } else if (SolrResponse.numberOfPartialErrors(partsRefToPartialErrorMap, getValues()) > 0) {
      	PartialErrors pes = new PartialErrors(ErrorCode.PRECONDITION_FAILED, "Some parts of the request resulted in errors - other parts might have succeeded. Client needs to check response for partial errors");
      	pes.setPayload(getValues());
      	err = pes;
      }
  	}

    return err;
  }

  public void addPartialError(String partRef, SolrException err) {
  	SolrResponse.addPartialError(partsRefToPartialErrorMap, getValues(), partRef, err);
  }
  
  public void copyFromSolrResponse(SolrResponse from) {
    List<String> handledPartsRef = from.getHandledPartsRef();
    if (handledPartsRef != null) {
      for (String handledPart : handledPartsRef) {
        addHandledPart(handledPart);
      }
    }
    Map<String, SolrException> pes = from.getPartialErrors();
    for (String key : pes.keySet()) {
      addPartialError(key, pes.get(key));
    }
    
    if (pes.size() == 0) {
      for (Map.Entry<String,Object> entry : from.getResponse()) {
        if (!entry.getKey().equals(SolrResponse.HANDLED_PARTS_KEY) && !entry.getKey().equals(SolrResponse.PARTIAL_ERRORS_KEY)) {
          getValues().add(entry.getKey(), entry.getValue());
        }
      }
    }
  }
  
  /**
   * The endtime of the request in milliseconds.
   * Used to calculate query time.
   * @see #setEndTime(long)
   * @see #getEndTime()
   */
  protected long endtime;

  /**
   * Get the time in milliseconds when the response officially finished. 
   */
  public long getEndTime() {
    if (endtime==0) {
      setEndTime();
    }
    return endtime;
  }

  /**
   * Stop the timer for how long this query took.
   * @see #setEndTime(long)
   */
  public long setEndTime() {
    return setEndTime(System.currentTimeMillis());
  }

  /**
   * Set the in milliseconds when the response officially finished. 
   * @see #setEndTime()
   */
  public long setEndTime(long endtime) {
    if (endtime!=0) {
      this.endtime=endtime;
    }
    return this.endtime;
  }
  
  /** Repsonse header to be logged */ 
  public NamedList<Object> getResponseHeader() {
    @SuppressWarnings("unchecked")
    SimpleOrderedMap<Object> header = (SimpleOrderedMap<Object>) values.get("responseHeader");
    return header;
  }
  
  /** Add a value to be logged.
   * 
   * @param name name of the thing to log
   * @param val value of the thing to log
   */
  public void addToLog(String name, Object val) {
    toLog.add(name, val);
  }
  
  /** Get loggable items.
   * 
   * @return things to log
   */
  public NamedList<Object> getToLog() {
    return toLog;
  }

  /** Returns a string of the form "logid name1=value1 name2=value2 ..." */
  public String getToLogAsString(String logid) {
    StringBuilder sb = new StringBuilder(logid);
    for (int i=0; i<toLog.size(); i++) {
      String name = toLog.getName(i);
      Object val = toLog.getVal(i);
      if (name != null) {
        sb.append(name).append('=');
      }
      sb.append(val).append(' ');
    }
    return sb.toString();
  }
  
  /**
   * Enables or disables the emission of HTTP caching headers for this response.
   * @param httpCaching true=emit caching headers, false otherwise
   */
  public void setHttpCaching(boolean httpCaching) {
    this.httpCaching=httpCaching;
  }
  
  /**
   * Should this response emit HTTP caching headers?
   * @return true=yes emit headers, false otherwise
   */
  public boolean isHttpCaching() {
    return this.httpCaching;
  }
}
