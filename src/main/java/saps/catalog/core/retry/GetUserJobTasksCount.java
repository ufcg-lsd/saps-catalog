package saps.catalog.core.retry;

import saps.catalog.core.Catalog;
import saps.catalog.core.retry.catalog.CatalogRetry;

public class GetUserJobTasksCount implements CatalogRetry<Integer> {
  private Catalog imageStore;
  private String state;
  private String search;
  private String jobId;
  private boolean allOngoingJobs;

  public GetUserJobTasksCount(Catalog imageStore, String jobId, String state, String search, boolean allOngoingJobs) {
    this.imageStore = imageStore;
    this.jobId = jobId;
    this.state = state;
    this.search = search;
    this.allOngoingJobs = allOngoingJobs;
  }

  @Override
  public Integer run() {
    return imageStore.getUserJobTasksCount(jobId, state, search, allOngoingJobs);
  }
}