package com.linkedin.thirdeye.taskexecution.impl.dataflow;

import com.google.common.base.Preconditions;
import com.linkedin.thirdeye.taskexecution.dataflow.writer.Writer;
import java.util.Collection;

public class CollectionWriter<T> implements Writer<T> {
  private Collection<T> storage;
  private volatile boolean isClosed;

  public CollectionWriter() {
  }

  public CollectionWriter(Collection<T> storage) {
    setStorage(storage);
  }

  public void setStorage(Collection<T> storage) {
    Preconditions.checkNotNull(storage);
    this.storage = storage;
  }

  @Override
  public void write(T o) {
    Preconditions.checkState(!isClosed, "Writer is closed.");
    if (storage != null) {
      storage.add(o);
    }
  }

  @Override
  public void close() {
    isClosed = true;
  }
}
