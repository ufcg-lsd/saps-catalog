package saps.catalog.core.retry.catalog;

import java.util.List;
import java.util.Date;
import saps.catalog.core.Catalog;
import saps.common.core.model.SapsLandsatImage;
import org.apache.log4j.Logger;

public class GetLandsatImages implements CatalogRetry<List<SapsLandsatImage>> {

    private static final Logger LOGGER = Logger.getLogger(GetLandsatImages.class);
    
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
	List<SapsLandsatImage> X = imageStore.getLandsatImages(region, date, landsat);
	LOGGER.info(X);
        return imageStore.getLandsatImages(region, date, landsat);
    }
}
