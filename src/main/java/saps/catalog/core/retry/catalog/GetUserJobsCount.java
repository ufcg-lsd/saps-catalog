package saps.catalog.core.retry.catalog;

import saps.catalog.core.Catalog;
import saps.common.core.model.enums.JobState;

public class GetUserJobsCount implements CatalogRetry<Integer> {
  private Catalog imageStore;
  private JobState state;
  private String search;
  private boolean recoverOnlyOngoing;

  public GetUserJobsCount(Catalog imageStore, JobState state, String search, boolean recoverOnlyOngoing) {
    this.imageStore = imageStore;
    this.state = state;
    this.search = search;
    this.recoverOnlyOngoing = recoverOnlyOngoing;
  }

  @Override
  public Integer run() {
    return imageStore.getUserJobsCount(state, search, recoverOnlyOngoing);
  }
}
