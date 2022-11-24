/* (C)2020 */
package saps.catalog.core.retry.catalog;

import saps.catalog.core.Catalog;

public class GetTasksCountCompleted implements CatalogRetry<Integer> {

  private Catalog imageStore;

  public GetTasksCountCompleted(Catalog imageStore) {
    this.imageStore = imageStore;
  }

  @Override
  public Integer run() {
    return imageStore.getTasksCountCompleted();
  }
}

