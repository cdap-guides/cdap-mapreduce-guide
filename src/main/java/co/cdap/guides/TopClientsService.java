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

import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.dataset.lib.ObjectStore;
import co.cask.cdap.api.service.AbstractService;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;

import java.util.List;
import javax.ws.rs.GET;
import javax.ws.rs.Path;

/**
 * Service to retrieve the results of topN IPs visited by count.
 */
public class TopClientsService extends AbstractService {

  public static final byte [] DATASET_RESULTS_KEY = {'r'};

  @Override
  protected void configure() {
    setName("TopClientsService");
    addHandler(new ResultsHandler());
  }

  public static class ResultsHandler extends AbstractHttpServiceHandler {

    @UseDataSet(LogAnalyticsApp.RESULTS_DATASET_NAME)
    private ObjectStore<List<ClientCount>> topN;

    @GET
    @Path("/results")
    public void getResults(HttpServiceRequest request, HttpServiceResponder responder) {

      List<ClientCount> result = topN.read(DATASET_RESULTS_KEY);
      if (result == null) {
        responder.sendError(404, "Result not found");
      } else {
        responder.sendJson(200, result);
      }
    }
  }
}
