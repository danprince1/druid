/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

window.consoleConfig = {
  exampleManifestsUrl: 'https://druid.apache.org/data/example-manifests-v2.tsv',
  defaultQueryContext: {
    priority: 20,
    greenfield: {
      user_id: 'DRUID_ROUTER_CONSOLE',
      workload_type: 'DRUID_ROUTER_CONSOLE',
    },
    brokerService: 'druid/hpdcBroker',
  },
  mandatoryQueryContext: {
    priority: 20,
    greenfield: {
      user_id: 'DRUID_ROUTER_CONSOLE',
      workload_type: 'DRUID_ROUTER_CONSOLE',
    },
    brokerService: 'druid/hpdcBroker',
  },
  /* future configs may go here */
};
