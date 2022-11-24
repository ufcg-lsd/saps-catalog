/* (C)2020 */
package saps.catalog.core.retry.catalog;

import saps.catalog.core.Catalog;

public class GetTasksCountOnGoing implements CatalogRetry<Integer> {

  private Catalog imageStore;

  public GetTasksCountOnGoing(Catalog imageStore) {
    this.imageStore = imageStore;
  }

  @Override
  public Integer run() {
    return imageStore.getTasksCountOnGoing();
  }
}

