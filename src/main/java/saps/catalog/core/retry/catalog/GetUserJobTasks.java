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
  private boolean recoverOnlyOngoing;

  public GetUserJobTasks(Catalog imageStore,
    String jobId,
    ImageTaskState state,
    String search,
    Integer page,
    Integer size,
    String sortField,
    String sortOrder,
    boolean recoverOnlyOngoing
  ) {
    this.imageStore = imageStore;
    this.jobId = jobId;
    this.state = state;
    this.search = search;
    this.page = page;
    this.size = size;
    this.sortField = sortField;
    this.sortOrder = sortOrder;
    this.recoverOnlyOngoing = recoverOnlyOngoing;
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
        recoverOnlyOngoing);
  }
}
