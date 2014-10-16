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

import co.cask.cdap.common.http.HttpRequest;
import co.cask.cdap.common.http.HttpRequests;
import co.cask.cdap.common.http.HttpResponse;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.StreamWriter;
import co.cask.cdap.test.TestBase;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Test for {@link LogAnalyticsApp}.
 */
public class LogAnalyticsAppTest extends TestBase {

  private static final Gson GSON = new Gson();

  @Test
  public void test() throws TimeoutException, InterruptedException, IOException {
    // Deploy the HelloWorld application
    ApplicationManager appManager = deployApplication(LogAnalyticsApp.class);

    // Send stream events to the logEvent Stream
    StreamWriter streamWriter = appManager.getStreamWriter("logEvents");
    streamWriter.send("255.255.255.185 - - [23/Sep/2014:11:45:38 -0400] \"GET /cdap.html HTTP/1.0\" 200 299 \" \" " +
                        "\"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)\"\n");
    streamWriter.send("255.255.255.185 - - [23/Sep/2014:11:45:38 -0400] \"GET /cask.html HTTP/1.0\" 200 296 \" \" " +
                        "\"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)\"\n");
    streamWriter.send("255.255.255.185 - - [23/Sep/2014:11:45:38 -0400] \"GET /tigon.html HTTP/1.0\" 401 2969 \" \" " +
                        "\"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)\"\n");

    streamWriter.send("255.255.255.182 - - [23/Sep/2014:11:45:38 -0400] \"GET /tigon.html HTTP/1.0\" 401 2969 \" \" " +
                        "\"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)\"\n");
    streamWriter.send("255.255.255.182 - - [23/Sep/2014:11:45:38 -0400] \"GET /tigon.html HTTP/1.0\" 401 2969 \" \" " +
                        "\"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)\"\n");


    TimeUnit.SECONDS.sleep(5);

    // Start MapReduce job and wait utmost 3 minutes to finish.
    MapReduceManager mapReduceManager = appManager.startMapReduce("TopClientsMapReduce");
    mapReduceManager.waitForFinish(3, TimeUnit.MINUTES);

    // Start the service.
    ServiceManager serviceManager = appManager.startService("TopClientsService");
    serviceStatusCheck(serviceManager, true);

    // Query results via HTTP.
    URL url = new URL(serviceManager.getServiceURL(), "results");
    HttpRequest request = HttpRequest.get(url).build();
    HttpResponse response = HttpRequests.execute(request);
    Assert.assertEquals(200, response.getResponseCode());

    response.getResponseBody();
    List<ClientCount> result = GSON.fromJson(response.getResponseBodyAsString(),
                                             new TypeToken<List<ClientCount>>() { }.getType());

    Assert.assertEquals(2, result.size());

    Assert.assertEquals(3, result.get(0).getCount());
    Assert.assertEquals("255.255.255.185", result.get(0).getClientIP());

    Assert.assertEquals(2, result.get(1).getCount());
    Assert.assertEquals("255.255.255.182", result.get(1).getClientIP());

    appManager.stopAll();
  }

  private void serviceStatusCheck(ServiceManager serviceManger, boolean running) throws InterruptedException {
    int trial = 0;
    while (trial++ < 5) {
      if (serviceManger.isRunning() == running) {
        return;
      }
      TimeUnit.SECONDS.sleep(1);
    }
    throw new IllegalStateException("Service state not executed. Expected " + running);
  }
}
