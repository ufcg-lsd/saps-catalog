package saps.catalog.core.retry.catalog;

import java.util.List;

import saps.common.core.model.SapsImage;
import saps.common.core.model.enums.ImageTaskState;

import saps.catalog.core.Catalog;

public class GetProcessingTasksRetry implements CatalogRetry<List<SapsImage>>{

	private Catalog imageStore;

	public GetProcessingTasksRetry(Catalog imageStore) {
		this.imageStore = imageStore;
	}
	
	@Override
	public List<SapsImage> run(){
		ImageTaskState[] states = {ImageTaskState.DOWNLOADING, ImageTaskState.PREPROCESSING, ImageTaskState.RUNNING};
		return imageStore.getTasksByState(states);
	}

}
