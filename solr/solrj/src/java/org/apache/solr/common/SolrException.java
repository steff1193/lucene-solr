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

package org.apache.solr.common;

import java.io.CharArrayWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Method;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.http.HttpResponse;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class SolrException extends RuntimeException {

	private static Logger log = LoggerFactory.getLogger(SolrException.class);
	protected NamedList<Object> payload = new SimpleOrderedMap<Object>();
	
  /**
   * This list of valid HTTP Status error codes that Solr may return in 
   * the case of a "Server Side" error.
   *
   * @since solr 1.2
   */
  public enum ErrorCode {
    BAD_REQUEST( 400 ),
    UNAUTHORIZED( 401 ),
    FORBIDDEN( 403 ),
    NOT_FOUND( 404 ),
    CONFLICT( 409 ),
    PRECONDITION_FAILED( 412 ),
    UNPROCESSABLE_ENTITY( 422 ),
    SERVER_ERROR( 500 ),
    SERVICE_UNAVAILABLE( 503 ),
    UNKNOWN(0);
    public final int code;
    
    private ErrorCode( int c )
    {
      code = c;
    }
    public static ErrorCode getErrorCode(int c){
      for (ErrorCode err : values()) {
        if(err.code == c) return err;
      }
      return UNKNOWN;
    }
  };

  public SolrException(ErrorCode code, String msg) {
    super(msg);
    this.code = code.code;
  }
  public SolrException(ErrorCode code, String msg, Throwable th) {
    super(msg, th);
    this.code = code.code;
  }

  public SolrException(ErrorCode code, Throwable th) {
    super(th);
    this.code = code.code;
  }

  /**
   * Constructor that can set arbitrary http status code. Not for 
   * use in Solr, but may be used by clients in subclasses to capture 
   * errors returned by the servlet container or other HTTP proxies.
   */
  protected SolrException(int code, String msg, Throwable th) {
    super(msg, th);
    this.code = code;
  }
  
  int code=0;

  /**
   * The HTTP Status code associated with this Exception.  For SolrExceptions 
   * thrown by Solr "Server Side", this should valid {@link ErrorCode}, 
   * however client side exceptions may contain an arbitrary error code based 
   * on the behavior of the Servlet Container hosting Solr, or any HTTP 
   * Proxies that may exist between the client and the server.
   *
   * @return The HTTP Status code associated with this Exception
   */
  public int code() { return code; }


  public void log(Logger log) { log(log,this); }
  public static void log(Logger log, Throwable e) {
    String stackTrace = toStr(e);
    String ignore = doIgnore(e, stackTrace);
    if (ignore != null) {
      log.info(ignore);
      return;
    }
    log.error(stackTrace);

  }

  public static void log(Logger log, String msg, Throwable e) {
    String stackTrace = msg + ':' + toStr(e);
    String ignore = doIgnore(e, stackTrace);
    if (ignore != null) {
      log.info(ignore);
      return;
    }
    log.error(stackTrace);
  }
  
  public static void log(Logger log, String msg) {
    String stackTrace = msg;
    String ignore = doIgnore(null, stackTrace);
    if (ignore != null) {
      log.info(ignore);
      return;
    }
    log.error(stackTrace);
  }

  // public String toString() { return toStr(this); }  // oops, inf loop
  @Override
  public String toString() { return super.toString(); }

  public static String toStr(Throwable e) {   
    CharArrayWriter cw = new CharArrayWriter();
    PrintWriter pw = new PrintWriter(cw);
    e.printStackTrace(pw);
    pw.flush();
    return cw.toString();

/** This doesn't work for some reason!!!!!
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    e.printStackTrace(pw);
    pw.flush();
    System.out.println("The STRING:" + sw.toString());
    return sw.toString();
**/
  }


  /** For test code - do not log exceptions that match any of the regular expressions in ignorePatterns */
  public static Set<String> ignorePatterns;

  /** Returns null if this exception does not match any ignore patterns, or a message string to use if it does. */
  public static String doIgnore(Throwable t, String m) {
    if (ignorePatterns == null || m == null) return null;
    if (t != null && t instanceof AssertionError) return null;

    for (String regex : ignorePatterns) {
      Pattern pattern = Pattern.compile(regex);
      Matcher matcher = pattern.matcher(m);
      
      if (matcher.find()) return "Ignoring exception matching " + regex;
    }

    return null;
  }
  
  public static Throwable getRootCause(Throwable t) {
    while (true) {
      Throwable cause = t.getCause();
      if (cause!=null) {
        t = cause;
      } else {
        break;
      }
    }
    return t;
  }

  public NamedList<Object> getPayload() {
    return payload;
  }
  
  private void setPayload(NamedList<Object> payload) {
    if (payload != null) {
      this.payload = payload;
    } else {
      this.payload.clear();
    }
  }
  
  public void setProperty(String name, Object val) {
    NamedList<Object> properties = getProperties(true);
    properties.add(name, val);
  }
  
  public Object getProperty(String name) {
    NamedList<Object> properties = getProperties(false);
    return (properties != null)?properties.get(name):null;
  }
  
  private NamedList<Object> getProperties(boolean create) {
    NamedList<Object> result = (NamedList<Object>)payload.get(PROPERTIES);
    if (create && result == null) {
      result = new SimpleOrderedMap<Object>();
      payload.add(PROPERTIES, result);
    }
    return result;
  }
  
  public static SolrException decodeFromNamedList(NamedList<Object> nl) {
    String errorType = (String)nl.get(ERROR_TYPE);
    Integer errorCode = (Integer)nl.get(ERROR_CODE);
    String errorMsg = (String)nl.get(ERROR_MSG);
    NamedList<Object> payload = new SimpleOrderedMap<Object>();
    payload.addAll(nl);
    payload.remove(ERROR_TYPE);
    payload.remove(ERROR_CODE);
    payload.remove(ERROR_MSG);
    return (SolrException)createFromClassNameCodeAndMsg(errorType, errorCode, errorMsg, payload);
  }
  
  public NamedList<Object> encodeInNamedList() {
    NamedList<Object> result = new SimpleOrderedMap<Object>();
    result.add(ERROR_CODE, code());
    result.add(ERROR_TYPE, getClass().getName());
    result.add(ERROR_MSG, (getMessage() == null)?"":getMessage());
    addPropertiesToParent(result);
    return result;
  }
  
  public boolean worthRetrying() {
    return false;
  }

  // --------- Encoding/decoding in String/NamedList/HttpServletResponse - start -------------------- //
  
	protected static final String ERROR_CODE = "error-code";
	// Using a neutral name ("error" not "exception" and "type" not "class") in order not to indicate any binding to java
	protected static final String ERROR_TYPE = "error-type";
	protected static final String ERROR_MSG = "error-msg";
	protected static final String HTTP_ERROR_HEADER_KEY = SolrResponse.HTTP_HEADER_KEY_PREFIX + ERROR_TYPE;
	protected static final String PROPERTIES = "properties";
	
  public static SolrException decodeFromHttpMethod(HttpResponse response, String reasonPhraseEncoding, String additionalMsgToPutInCreatedException, NamedList<Object> payload) throws UnsupportedEncodingException {
    return createFromClassNameCodeAndMsg(response.getFirstHeader(HTTP_ERROR_HEADER_KEY).getValue(), response.getStatusLine().getStatusCode(), java.net.URLDecoder.decode(response.getStatusLine().getReasonPhrase(), reasonPhraseEncoding) + additionalMsgToPutInCreatedException, payload);
  }
  
  public void encodeTypeInHttpServletResponse(Object httpServletResponse) {
    // httpServletResponse.addHeader(HTTP_ERROR_HEADER_KEY, getClass().getName());
    // Coded using reflection, because SolrException needs to be in client jar as well as in server jar, but you only want to call
    // encodeTypeInHttpServletResponse on server-side, and you dont want to need to include jar including HttpServletResponse on client classpath 
    try {
      Method m = httpServletResponse.getClass().getMethod("addHeader", String.class, String.class);
      m.invoke(httpServletResponse, HTTP_ERROR_HEADER_KEY, getClass().getName());
    } catch (Exception e) {
      if (e instanceof RuntimeException) throw (RuntimeException)e;
      throw new RuntimeException(e);
    }
  }
  
  public boolean addPropertiesToParent(NamedList<Object> parentNamedList) {
    NamedList<Object> properties = getProperties(false);
    if (properties != null) {
      parentNamedList.add(PROPERTIES, properties);
      return true;
    }
    return false;
  }
  
  protected static SolrException createFromClassNameCodeAndMsg(String className, int code, String msg, NamedList<Object> payload) {
		if (className != null) {
  		try {
    		Class clazz = Class.forName(className);
    		if (SolrException.class.isAssignableFrom(clazz)) {
    			SolrException result = ((Class<SolrException>)clazz).getConstructor(ErrorCode.class, String.class).newInstance(SolrException.ErrorCode.getErrorCode(code), msg);
    			result.setPayload(payload);
    			return result;
    		}
      } catch (ClassNotFoundException e) {
      	log.warn("Could not create exception of type " + className + ". Class not found.");
      } catch (NoSuchMethodException e) {
      	log.warn("Could not create exception of type " + className + ". Does not have constructor taking ErrorCode and String");
      } catch (Exception e) {
      	log.warn("Could not create exception of type " + className + ".", e);
      }
		}
    return new SolrException(SolrException.ErrorCode.getErrorCode(code), msg);
	}
  
  // --------- Encoding/decoding in String/NamedList/HttpServletResponse - end -------------------- //

}
