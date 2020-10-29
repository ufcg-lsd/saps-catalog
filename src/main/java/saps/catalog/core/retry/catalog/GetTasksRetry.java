package saps.catalog.core.retry.catalog;

import java.util.List;

import saps.common.core.model.SapsImage;
import saps.common.core.model.enums.ImageTaskState;

import saps.catalog.core.Catalog;

public class GetTasksRetry implements CatalogRetry<List<SapsImage>>{

	private Catalog imageStore;
	private ImageTaskState[] states;
	
	public GetTasksRetry(Catalog imageStore, ImageTaskState state) {
		this.imageStore = imageStore;
		this.states = new ImageTaskState[]{state};
	}
	
	@Override
	public List<SapsImage> run() {
		return imageStore.getTasksByState(states);
	}

}
