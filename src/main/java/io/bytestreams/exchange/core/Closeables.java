package io.bytestreams.exchange.core;

import java.io.Closeable;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Closeables {
  private static final Logger log = LoggerFactory.getLogger(Closeables.class);

  private Closeables() {}

  static void closeQuietly(Closeable closeable) {
    if (closeable != null) {
      try {
        closeable.close();
      } catch (IOException e) {
        log.trace("Exception during closeQuietly", e);
      }
    }
  }
}
