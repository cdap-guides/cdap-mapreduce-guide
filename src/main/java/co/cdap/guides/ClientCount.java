package co.cdap.guides;

import com.google.common.base.Objects;

/**
 * Pojo to store clientIP and count.
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
  public String toString() {
    return Objects.toStringHelper(this)
                    .add("ip", clientIP)
                    .add("count", count)
                    .toString();
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
