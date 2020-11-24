/* (C)2020 */
package saps.catalog.core.retry.catalog;

import saps.catalog.core.Catalog;
import saps.common.core.model.SapsImage;

public class AddTimestampRetry implements CatalogRetry<Void> {

  private Catalog imageStore;
  private SapsImage task;

  public AddTimestampRetry(Catalog imageStore, SapsImage task) {
    this.imageStore = imageStore;
    this.task = task;
  }

  @Override
  public Void run() {
    task.setUpdateTime(imageStore.getTaskById(task.getTaskId()).getUpdateTime());
    imageStore.addStateChangeTime(task.getTaskId(), task.getState(), task.getUpdateTime());
    return null;
  }
}
