package saps.catalog.core.retry.catalog;

import saps.catalog.core.Catalog;
import saps.common.core.model.enums.ImageTaskState;

public class GetUserJobTasksCount implements CatalogRetry<Integer> {
  private Catalog imageStore;
  private ImageTaskState state;
  private String search;
  private String jobId;
  private boolean recoverOnlyOngoing;

  public GetUserJobTasksCount(Catalog imageStore, String jobId, ImageTaskState state, String search, boolean recoverOnlyOngoing) {
    this.imageStore = imageStore;
    this.jobId = jobId;
    this.state = state;
    this.search = search;
    this.recoverOnlyOngoing = recoverOnlyOngoing;
  }

  @Override
  public Integer run() {
    return imageStore.getUserJobTasksCount(jobId, state, search, recoverOnlyOngoing);
  }
}