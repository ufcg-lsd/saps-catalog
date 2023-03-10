package saps.catalog.core.retry.catalog;

import java.util.List;

import saps.catalog.core.Catalog;
import saps.common.core.model.SapsImage;

public class GetUserJobTasks implements CatalogRetry<List<SapsImage>> {
  
  private Catalog imageStore;
  private String jobId;
  private String state;
  private String search;
  private Integer page;
  private Integer size;
  private String sortField;
  private String sortOrder;
  private boolean allOngoingJobs;

  public GetUserJobTasks(Catalog imageStore,
    String jobId,
    String state,
    String search,
    Integer page,
    Integer size,
    String sortField,
    String sortOrder,
    boolean allOngoingJobs
  ) {
    this.imageStore = imageStore;
    this.jobId = jobId;
    this.state = state;
    this.search = search;
    this.page = page;
    this.size = size;
    this.sortField = sortField;
    this.sortOrder = sortOrder;
    this.allOngoingJobs = allOngoingJobs;
  }

  @Override
  public List<SapsImage> run() {
    return imageStore.getUserJobTasks(
        jobId,
        state,
        search,
        page,
        size,
        sortField,
        sortOrder,
        allOngoingJobs);
  }
}
