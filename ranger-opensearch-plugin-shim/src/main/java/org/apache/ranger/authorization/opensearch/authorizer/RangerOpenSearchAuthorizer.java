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

package org.apache.ranger.authorization.opensearch.authorizer;

import java.util.List;

import org.apache.ranger.plugin.classloader.RangerPluginClassLoader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class RangerOpenSearchAuthorizer {

    private static final Logger LOG = LogManager.getLogger(RangerOpenSearchAuthorizer.class);

    private static final String RANGER_PLUGIN_TYPE = "opensearch";
    private static final String RANGER_OPEN_SEARCH_AUTHORIZER_IMPL_CLASSNAME = "org.apache.ranger.authorization.opensearch.authorizer.RangerOpenSearchAuthorizer";

    private static RangerPluginClassLoader rangerPluginClassLoader = null;
    private static ClassLoader osClassLoader = null;

    private RangerOpenSearchAccessControl rangerOpenSearchAccessControl = null;

    public RangerOpenSearchAuthorizer() {
        LOG.debug("==> RangerOpenSearchAuthorizer.RangerOpenSearchAuthorizer()");
        this.init();
        LOG.debug("<== RangerOpenSearchAuthorizer.RangerOpenSearchAuthorizer()");

    }

    public void init() {
        LOG.debug("==> RangerOpenSearchAuthorizer.init()");
        try {
            osClassLoader = Thread.currentThread().getContextClassLoader();
            Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());

            rangerPluginClassLoader = RangerPluginClassLoader.getInstance(RANGER_PLUGIN_TYPE, this.getClass());
            Thread.currentThread().setContextClassLoader(osClassLoader);
            @SuppressWarnings("unchecked")
            Class<RangerOpenSearchAccessControl> cls = (Class<RangerOpenSearchAccessControl>) Class
                    .forName(RANGER_OPEN_SEARCH_AUTHORIZER_IMPL_CLASSNAME, true, rangerPluginClassLoader);
            activatePluginClassLoader();
            rangerOpenSearchAccessControl = cls.newInstance();
        } catch (Exception e) {
            LOG.error("Error Enabling RangerOpenSearchAuthorizer", e);
        } finally {
            deactivatePluginClassLoader();
        }

        LOG.debug("<== RangerOpenSearchAuthorizer.init()");
    }

    public boolean checkPermission(String user, List<String> groups, String index, String action,
                                   String clientIPAddress) {
        boolean ret = false;

        LOG.debug("==> RangerOpenSearchAuthorizer.checkPermission()");

        try {
            activatePluginClassLoader();

            ret = rangerOpenSearchAccessControl.checkPermission(user, groups, index, action, clientIPAddress);
        } finally {
            deactivatePluginClassLoader();
        }

        LOG.debug("<== RangerOpenSearchAuthorizer.checkPermission()");

        return ret;
    }

    private void activatePluginClassLoader() {
        if (rangerPluginClassLoader != null) {
            Thread.currentThread().setContextClassLoader(rangerPluginClassLoader);
        }
    }

    private void deactivatePluginClassLoader() {
        if (osClassLoader != null) {
            Thread.currentThread().setContextClassLoader(osClassLoader);
        }
    }
}
