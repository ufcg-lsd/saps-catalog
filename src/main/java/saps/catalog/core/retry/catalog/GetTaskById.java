/* (C)2020 */
package saps.catalog.core.retry.catalog;

import saps.catalog.core.Catalog;
import saps.common.core.model.SapsImage;

public class GetTaskById implements CatalogRetry<SapsImage> {

  private Catalog imageStore;
  private String taskId;

  public GetTaskById(Catalog imageStore, String taskId) {
    this.imageStore = imageStore;
    this.taskId = taskId;
  }

  @Override
  public SapsImage run() {
    return imageStore.getTaskById(taskId);
  }
}
