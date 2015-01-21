/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package co.cdap.guides;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.ObjectStore;
import co.cask.cdap.api.dataset.lib.ObjectStores;
import com.google.common.base.Throwables;
import com.google.inject.util.Types;


/**
 * LogAnalyticsApp that consumes Apache access log event, computes topN clientIps by traffic and a service
 * to retrieve the results.
 */
public class LogAnalyticsApp extends AbstractApplication {

  public static final String RESULTS_DATASET_NAME = "resultStore";

  @Override
  public void configure() {
    setName("LogAnalyticsApp");
    addStream(new Stream("logEvents"));
    addMapReduce(new TopClientsMapReduce());
    addService(new TopClientsService());
    try {
      DatasetProperties props = ObjectStores.objectStoreProperties(Types.listOf(ClientCount.class),
                                                                   DatasetProperties.EMPTY);
      createDataset(RESULTS_DATASET_NAME, ObjectStore.class, props);
    } catch (UnsupportedTypeException e) {
      throw Throwables.propagate(e);
    }
  }
}
