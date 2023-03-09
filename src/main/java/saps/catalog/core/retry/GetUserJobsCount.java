package saps.catalog.core.retry;

import saps.catalog.core.Catalog;
import saps.catalog.core.retry.catalog.CatalogRetry;

public class GetUserJobsCount implements CatalogRetry<Integer> {
  private Catalog imageStore;
  private String state;
  private String search;
  private boolean allOngoingJobs;

  public GetUserJobsCount(Catalog imageStore, String state, String search, boolean allOngoingJobs) {
    this.imageStore = imageStore;
    this.state = state;
    this.search = search;
    this.allOngoingJobs = allOngoingJobs;
  }

  @Override
  public Integer run() {
    return imageStore.getUserJobsCount(state, search, allOngoingJobs);
  }
}
