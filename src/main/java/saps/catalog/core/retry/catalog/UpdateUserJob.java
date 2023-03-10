package saps.catalog.core.retry.catalog;

import saps.catalog.core.Catalog;
import saps.common.core.model.enums.JobState;

public class UpdateUserJob implements CatalogRetry<Boolean> {

  private Catalog imageStore;
  private String jobId;
  private JobState state;

  public UpdateUserJob(Catalog imageStore, String jobId, JobState state) {
    this.imageStore = imageStore;
    this.jobId = jobId;
    this.state = state;
  }

  @Override
  public Boolean run() {
    imageStore.updateUserJob(jobId, state);
    return true;
  }
}
