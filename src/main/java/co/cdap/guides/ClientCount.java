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

/**
 * Holds clientIP, count pairs.
 */
public class ClientCount implements Comparable<ClientCount> {
  private final String clientIP;
  private final int count;

  public ClientCount(String clientIP, int count) {
    this.clientIP = clientIP;
    this.count = count;
  }

  public String getClientIP() {
    return clientIP;
  }

  public int getCount() {
    return count;
  }

  @Override
  public int compareTo(ClientCount clientCount) {
    if (clientCount.count == count) {
      return 0;
    } else {
      return count > clientCount.count ? -1 : 1;
    }
  }
}
