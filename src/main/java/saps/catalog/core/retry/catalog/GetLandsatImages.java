package saps.catalog.core.retry.catalog;

import java.util.Date;
import saps.catalog.core.Catalog;
import saps.common.core.model.SapsLandsatImage;

public class GetLandsatImages implements CatalogRetry<SapsLandsatImage> {

  private Catalog imageStore;
  private String region;
  private Date date;

  public GetLandsatImages(Catalog imageStore, String region, Date date) {
    this.imageStore = imageStore;
    this.region = region;
    this.date = date;
  }

  @Override
  public SapsLandsatImage run() {
    SapsLandsatImage X = imageStore.getLandsatImages(region, date);
    return X;
  }
}
