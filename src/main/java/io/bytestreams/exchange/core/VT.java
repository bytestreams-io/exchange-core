package io.bytestreams.exchange.core;

/** Utility for creating named virtual threads. */
final class VT {
  private VT() {}

  /**
   * Creates an unstarted named virtual thread.
   *
   * <p>Virtual threads are lightweight, scheduled by the JVM rather than the OS. They are suitable
   * for blocking I/O loops (read/write) where the thread spends most of its time waiting.
   *
   * @param task the runnable to execute
   * @param name the thread name (visible in thread dumps and diagnostics)
   * @return the unstarted virtual thread
   */
  static Thread of(Runnable task, String name) {
    return Thread.ofVirtual().name(name).unstarted(task);
  }

  /**
   * Creates and starts a named virtual thread.
   *
   * @param task the runnable to execute
   * @param name the thread name (visible in thread dumps and diagnostics)
   * @return the started virtual thread
   */
  static Thread start(Runnable task, String name) {
    return Thread.ofVirtual().name(name).start(task);
  }
}
