/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.ranger.services.opensearch.client;

import java.util.List;
import java.util.Map;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.ranger.plugin.service.ResourceLookupContext;

public class OpenSearchResourceMgr {

    public static final String INDEX = "index";

    private static final Logger LOG = Logger.getLogger(OpenSearchResourceMgr.class);

    public static Map<String, Object> validateConfig(String serviceName, Map<String, String> configs) throws Exception {
        Map<String, Object> ret = null;

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> OpenSearchResourceMgr.validateConfig() serviceName: " + serviceName + ", configs: "
                    + configs);
        }

        try {
            ret = OpenSearchClient.connectionTest(serviceName, configs);
        } catch (Exception e) {
            LOG.error("<== OpenSearchResourceMgr.validateConfig() error: " + e);
            throw e;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== OpenSearchResourceMgr.validateConfig() result: " + ret);
        }
        return ret;
    }

    public static List<String> getOpenSearchResources(String serviceName,
                                                      Map<String, String> configs,
                                                      ResourceLookupContext context) {
        String userInput = context.getUserInput();
        String resource = context.getResourceName();
        Map<String, List<String>> resourceMap = context.getResources();
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> OpenSearchResourceMgr.getOpenSearchResources()  userInput: " + userInput
                    + ", resource: " + resource + ", resourceMap: " + resourceMap);
        }

        if (MapUtils.isEmpty(configs)) {
            LOG.error("Connection config is empty!");
            return null;
        }

        if (StringUtils.isEmpty(userInput)) {
            LOG.warn("User input is empty, set default value : *");
            userInput = "*";
        }

        final OpenSearchClient openSearchClient = OpenSearchClient.getOpenSearchClient(serviceName, configs);
        if (openSearchClient == null) {
            LOG.error("Failed to getOpenSearchResources!");
            return null;
        }

        List<String> resultList = null;

        if (StringUtils.isNotEmpty(resource)) {
            switch (resource) {
                case INDEX:
                    List<String> existingConnectors = resourceMap.get(INDEX);
                    resultList = openSearchClient.getIndexList(userInput, existingConnectors);
                    break;
                default:
                    break;
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== OpenSearchResourceMgr.getOpenSearchResources() result: " + resultList);
        }
        return resultList;
    }

}
