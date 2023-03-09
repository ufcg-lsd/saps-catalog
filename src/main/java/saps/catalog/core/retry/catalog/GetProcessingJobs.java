/* (C)2020 */
package saps.catalog.core.retry.catalog;

import java.util.List;
import saps.catalog.core.Catalog;
import saps.common.core.model.SapsUserJob;
import saps.common.core.model.enums.JobState;

public class GetProcessingJobs implements CatalogRetry<List<SapsUserJob>> {

  private Catalog imageStore;

  public GetProcessingTasksRetry(Catalog imageStore) {
    this.imageStore = imageStore;
  }

  @Override
  public List<SapsImage> run() {
    JobState[] states = {
     JobState.SUBMITTED, JobState.PREPROCESSING, JobState.CREATED, JobState.RUNNING
    };
    return imageStore.getJobsByState(states);
  }
}
