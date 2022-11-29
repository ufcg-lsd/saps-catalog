package saps.catalog.core.retry.catalog;

import java.util.List;
import java.util.Date;
import saps.catalog.core.Catalog;
import saps.common.core.model.SapsLandsatImage;

public class GetLandsatImages implements CatalogRetry<List<SapsLandsatImage>> {
    
    private Catalog imageStore;
    private String region;
    private Date date;
    private String landsat;

    public GetLandsatImages(Catalog imageStore, String region, Date date, String landsat) {
        this.imageStore = imageStore;
        this.region = region;
        this.date = date;
        this.landsat = landsat;
    }

    @Override
    public List<SapsLandsatImage> run() {
        return imageStore.getLandsatImages(region, date, landsat);
    }
}
