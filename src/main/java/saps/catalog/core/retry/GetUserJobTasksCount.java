package saps.catalog.core.retry;

import saps.catalog.core.Catalog;
import saps.catalog.core.retry.catalog.CatalogRetry;

public class GetUserJobTasksCount implements CatalogRetry<Integer> {
  private Catalog imageStore;
  private String state;
  private String search;
  private String jobId;

  public GetUserJobTasksCount(Catalog imageStore, String jobId, String state, String search) {
    this.imageStore = imageStore;
    this.jobId = jobId;
    this.state = state;
    this.search = search;
  }

  @Override
  public Integer run() {
    return imageStore.getUserJobTasksCount(jobId, state, search);
  }
}