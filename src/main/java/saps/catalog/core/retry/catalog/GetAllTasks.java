/* (C)2020 */
package saps.catalog.core.retry.catalog;

import java.util.List;
import saps.catalog.core.Catalog;
import saps.common.core.model.SapsImage;

public class GetAllTasks implements CatalogRetry<List<SapsImage>> {

  private Catalog imageStore;

  public GetAllTasks(Catalog imageStore) {
    this.imageStore = imageStore;
  }

  @Override
  public List<SapsImage> run() {
    return imageStore.getAllTasks();
  }
}
