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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.audit.provider.MiscUtil;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.apache.ranger.services.opensearch.client.OpenSearchResourceMgr;
import org.apache.ranger.services.opensearch.privilege.IndexPrivilegeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

public class RangerOpenSearchAuthorizer implements RangerOpenSearchAccessControl {

	private static final Logger LOG = LoggerFactory.getLogger(RangerOpenSearchAuthorizer.class);

	private static volatile RangerOpenSearchInnerPlugin opensearchPlugin = null;

	public RangerOpenSearchAuthorizer() {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerOpenSearchAuthorizer.RangerOpenSearchAuthorizer()");
		}

		this.init();

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerOpenSearchAuthorizer.RangerOpenSearchAuthorizer()");
		}
	}

	public void init() {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerOpenSearchAuthorizer.init()");
		}

		RangerOpenSearchInnerPlugin plugin = opensearchPlugin;

		if (plugin == null) {
			synchronized (RangerOpenSearchAuthorizer.class) {
				plugin = opensearchPlugin;

				if (plugin == null) {
					plugin = new RangerOpenSearchInnerPlugin();
					plugin.init();
					opensearchPlugin = plugin;
				}
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerOpenSearchAuthorizer.init()");
		}
	}

	@Override
	public boolean checkPermission(String user, List<String> groups, String index, String action,
			String clientIPAddress) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerOpenSearchAuthorizer.checkPermission( user=" + user + ", groups=" + groups
					+ ", index=" + index + ", action=" + action + ", clientIPAddress=" + clientIPAddress + ")");
		}

		boolean ret = false;

		if (opensearchPlugin != null) {
			if (null == groups) {
				groups = new ArrayList <>(MiscUtil.getGroupsForRequestUser(user));
			}
			String privilege = IndexPrivilegeUtils.getPrivilegeFromAction(action);
			RangerOpenSearchAccessRequest request = new RangerOpenSearchAccessRequest(user, groups, index,
					privilege, clientIPAddress);

			RangerAccessResult result = opensearchPlugin.isAccessAllowed(request);
			if (result != null && result.getIsAllowed()) {
				ret = true;
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerOpenSearchAuthorizer.checkPermission(): result=" + ret);
		}

		return ret;
	}
}

class RangerOpenSearchInnerPlugin extends RangerBasePlugin {
	public RangerOpenSearchInnerPlugin() {
		super("opensearch", "opensearch");
	}

	@Override
	public void init() {
		super.init();

		RangerOpenSearchAuditHandler auditHandler = new RangerOpenSearchAuditHandler(getConfig());

		super.setResultProcessor(auditHandler);
	}
}

class RangerOpenSearchResource extends RangerAccessResourceImpl {
	public RangerOpenSearchResource(String index) {
		if (StringUtils.isEmpty(index)) {
			index = "*";
		}
		setValue(OpenSearchResourceMgr.INDEX, index);
	}
}

class RangerOpenSearchAccessRequest extends RangerAccessRequestImpl {
	public RangerOpenSearchAccessRequest(String user, List<String> groups, String index, String privilege,
			String clientIPAddress) {
		super.setUser(user);
		if (CollectionUtils.isNotEmpty(groups)) {
			super.setUserGroups(Sets.newHashSet(groups));
		}
		super.setResource(new RangerOpenSearchResource(index));
		super.setAccessType(privilege);
		super.setAction(privilege);
		super.setClientIPAddress(clientIPAddress);
		super.setAccessTime(new Date());
	}
}
