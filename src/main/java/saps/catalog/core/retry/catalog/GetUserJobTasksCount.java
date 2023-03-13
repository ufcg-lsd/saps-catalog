package saps.catalog.core.retry.catalog;

import saps.catalog.core.Catalog;
import saps.common.core.model.enums.ImageTaskState;

public class GetUserJobTasksCount implements CatalogRetry<Integer> {
  private Catalog imageStore;
  private ImageTaskState state;
  private String search;
  private String jobId;
  private boolean recoverOngoing;
  private boolean recoverCompleted;

  public GetUserJobTasksCount(Catalog imageStore, String jobId, ImageTaskState state, String search, boolean recoverOngoing, boolean recoverCompleted) {
    this.imageStore = imageStore;
    this.jobId = jobId;
    this.state = state;
    this.search = search;
    this.recoverOngoing = recoverOngoing;
    this.recoverCompleted = recoverCompleted;
  }

  @Override
  public Integer run() {
    return imageStore.getUserJobTasksCount(jobId, state, search, recoverOngoing, recoverCompleted);
  }
}