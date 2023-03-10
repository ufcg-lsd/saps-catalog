package saps.catalog.core.retry.catalog;

import java.util.List;

import saps.catalog.core.Catalog;
import saps.common.core.model.SapsUserJob;
import saps.common.core.model.enums.JobState;

public class GetUserJobs implements CatalogRetry<List<SapsUserJob>> {
  private Catalog imageStore;
  private JobState state;
  private String search;
  private Integer page;
  private Integer size;
  private String sortField;
  private String sortOrder;
  private boolean withoutTasks;
  private boolean recoverOnlyOngoing;

  public GetUserJobs(Catalog imageStore,
      JobState state,
      String search,
      Integer page,
      Integer size,
      String sortField,
      String sortOrder,
      boolean withoutTasks,
      boolean recoverOnlyOngoing) {
    this.imageStore = imageStore;
    this.state = state;
    this.search = search;
    this.page = page;
    this.size = size;
    this.sortField = sortField;
    this.sortOrder = sortOrder;
    this.withoutTasks = withoutTasks;
    this.recoverOnlyOngoing = recoverOnlyOngoing;
  }

  @Override
  public List<SapsUserJob> run() {
    return imageStore.getUserJobs(
        state,
        search,
        page,
        size,
        sortField,
        sortOrder,
        withoutTasks,
        recoverOnlyOngoing);
  }
}
