package saps.catalog.core.retry.catalog;

import java.util.Date;
import java.util.List;

import saps.common.core.model.SapsImage;
import saps.common.core.model.enums.ImageTaskState;

import saps.catalog.core.Catalog;

public class GetProcessedTasks implements CatalogRetry<List<SapsImage>> {

	private Catalog imageStore;
	private String region;
	private Date initDate;
	private Date endDate;
	private String inputdownloadingPhaseTag;
	private String preprocessingPhaseTag;
	private String processingPhaseTag;

	public GetProcessedTasks(Catalog imageStore, String region, Date initDate, Date endDate,
			String inputdownloadingPhaseTag, String preprocessingPhaseTag, String processingPhaseTag) {
		this.imageStore = imageStore;
		this.region = region;
		this.initDate = initDate;
		this.endDate = endDate;
		this.inputdownloadingPhaseTag = inputdownloadingPhaseTag;
		this.preprocessingPhaseTag = preprocessingPhaseTag;
		this.processingPhaseTag = processingPhaseTag;
	}

	@Override
	public List<SapsImage> run(){
		return imageStore.filterTasks(ImageTaskState.ARCHIVED, region, initDate, endDate, inputdownloadingPhaseTag, preprocessingPhaseTag,
				processingPhaseTag);
	}

}
