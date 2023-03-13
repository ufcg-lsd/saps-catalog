package saps.catalog.core.retry.catalog;

import java.util.List;

import saps.catalog.core.Catalog;
import saps.common.core.model.SapsImage;
import saps.common.core.model.enums.ImageTaskState;

public class GetUserJobTasks implements CatalogRetry<List<SapsImage>> {
  
  private Catalog imageStore;
  private String jobId;
  private ImageTaskState state;
  private String search;
  private Integer page;
  private Integer size;
  private String sortField;
  private String sortOrder;
  private boolean recoverOngoing;
  private boolean recoverCompleted;

  public GetUserJobTasks(Catalog imageStore,
    String jobId,
    ImageTaskState state,
    String search,
    Integer page,
    Integer size,
    String sortField,
    String sortOrder,
    boolean recoverOngoing,
    boolean recoverCompleted
  ) {
    this.imageStore = imageStore;
    this.jobId = jobId;
    this.state = state;
    this.search = search;
    this.page = page;
    this.size = size;
    this.sortField = sortField;
    this.sortOrder = sortOrder;
    this.recoverOngoing = recoverOngoing;
    this.recoverCompleted = recoverCompleted;
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
        recoverOngoing,
        recoverCompleted);
  }
}
