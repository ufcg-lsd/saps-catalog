package saps.catalog.core.retry.catalog;

import saps.catalog.core.Catalog;
import saps.common.core.model.enums.JobState;

public class GetUserJobsCount implements CatalogRetry<Integer> {
  private Catalog imageStore;
  private JobState state;
  private String search;
  private boolean recoverOngoing;
  private boolean recoverCompleted;

  public GetUserJobsCount(Catalog imageStore, JobState state, String search, boolean recoverOngoing, boolean recoverCompleted) {
    this.imageStore = imageStore;
    this.state = state;
    this.search = search;
    this.recoverOngoing = recoverOngoing;
    this.recoverCompleted = recoverCompleted;
  }

  @Override
  public Integer run() {
    return imageStore.getUserJobsCount(state, search, recoverOngoing, recoverCompleted);
  }
}
