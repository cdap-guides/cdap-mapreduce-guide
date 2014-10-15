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

  @Override
  protected void configure() {
    setName("TopClientsService");
    addHandler(new ResultsHandler());
  }

  public static class ResultsHandler extends AbstractHttpServiceHandler {

    @UseDataSet(LogAnalyticsApp.DATASET_NAME)
    private ObjectStore<List<ClientCount>> topN;

    @GET
    @Path("/results")
    public void getResults(HttpServiceRequest request, HttpServiceResponder responder) {

      List<ClientCount> result = topN.read(LogAnalyticsApp.DATASET_RESULTS_KEY);
      if (result == null) {
        responder.sendError(404, "Result not found");
      } else {
        responder.sendJson(200, result);
      }
    }
  }
}
