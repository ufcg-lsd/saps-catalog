/* (C)2020 */
package saps.catalog.core.retry.catalog;

import java.lang.Integer;
import java.util.List;
import saps.catalog.core.Catalog;
import saps.common.core.model.SapsImage;

public class GetTasksCompletedWithPagination implements CatalogRetry<List<SapsImage>> {

  private Catalog imageStore;
  private Integer page;
  private Integer size;
  private String sortField;
  private String sortOrder;
  private String search;

  public GetTasksCompletedWithPagination(Catalog imageStore, String search, Integer page, Integer size,
      String sortField, String sortOrder) {
    this.imageStore = imageStore;
    this.page = page;
    this.size = size;
    this.search = search;
    this.sortField = sortField;
    this.sortOrder = sortOrder;
  }


  @Override
  public List<SapsImage> run() {
    return imageStore.getTasksCompletedWithPagination(search, page, size, sortField, sortOrder);
  }
}

