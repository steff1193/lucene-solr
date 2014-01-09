package org.apache.solr.common.cloud;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.StringUtils;
import org.apache.solr.common.cloud.ZkCredentialsProvider.ZkCredentials;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO: improve backoff retry impl
 */
public class DefaultConnectionStrategy extends ZkClientConnectionStrategy {

  private static Logger log = LoggerFactory.getLogger(DefaultConnectionStrategy.class);
  
  private ZkCredentialsProvider zkCredentialsToAddAutomatically;
  private boolean zkCredentialsToAddAutomaticallyUsed;

  public DefaultConnectionStrategy() {
    zkCredentialsToAddAutomatically = createZkCredentialsToAddAutomatically();
    zkCredentialsToAddAutomaticallyUsed = false;
  }

  @Override
  public void connect(String serverAddress, int timeout, Watcher watcher, ZkUpdate updater) throws IOException, InterruptedException, TimeoutException {
    updater.update(createSolrZooKeeper(serverAddress, timeout, watcher));
  }

  @Override
  public void reconnect(final String serverAddress, final int zkClientTimeout,
      final Watcher watcher, final ZkUpdate updater) throws IOException {
    log.info("Connection expired - starting a new one...");
    
    try {
      updater
          .update(createSolrZooKeeper(serverAddress, zkClientTimeout, watcher));
      log.info("Reconnected to ZooKeeper");
    } catch (Exception e) {
      SolrException.log(log, "Reconnect to ZooKeeper failed", e);
      log.info("Reconnect to ZooKeeper failed");
    }
    
  }

  protected SolrZooKeeper createSolrZooKeeper(final String serverAddress, final int zkClientTimeout,
      final Watcher watcher) throws IOException {
    SolrZooKeeper result = new SolrZooKeeper(serverAddress, zkClientTimeout, watcher);
    
    zkCredentialsToAddAutomaticallyUsed = true;
    for (ZkCredentials zkCredentials : zkCredentialsToAddAutomatically.getCredentials()) {
      result.addAuthInfo(zkCredentials.getScheme(), zkCredentials.getAuth());
    }

    return result;
  }
  
  public void setZkCredentialsToAddAutomatically(ZkCredentialsProvider zkCredentialsToAddAutomatically) {
    if (zkCredentialsToAddAutomaticallyUsed || (zkCredentialsToAddAutomatically == null)) 
      throw new RuntimeException("Cannot change zkCredentialsToAddAutomatically after it has been (connect or reconnect was called) used or to null");
    this.zkCredentialsToAddAutomatically = zkCredentialsToAddAutomatically;
  }
  
  public static final String ZK_CREDENTIALS_PROVIDER_CLASS_NAME_VM_PARAM_NAME = "defaultZkCredentialsProvider";
  protected ZkCredentialsProvider createZkCredentialsToAddAutomatically() {
    String zkCredentialsProviderClassName = System.getProperty(ZK_CREDENTIALS_PROVIDER_CLASS_NAME_VM_PARAM_NAME);
    if (!StringUtils.isEmpty(zkCredentialsProviderClassName)) {
      try {
        return (ZkCredentialsProvider)Class.forName(zkCredentialsProviderClassName).getConstructor().newInstance();
      } catch (Throwable t) {
        // just ignore - go default
        log.warn("VM param zkACLProvider does not point to a class implementing ZkACLProvider and with a non-arg constructor", t);
      }
    }
    return new DefaultZkCredentialsProvider();
  }

  
}
