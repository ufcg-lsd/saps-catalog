package saps.catalog.core.retry.catalog;

import java.util.Date;
import saps.catalog.core.Catalog;
import saps.common.core.model.SapsLandsatImage;
import org.apache.log4j.Logger;

public class GetLandsatImages implements CatalogRetry<SapsLandsatImage> {

    private static final Logger LOGGER = Logger.getLogger(GetLandsatImages.class);
    
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
	LOGGER.info(X);
        return X;
    }
}
