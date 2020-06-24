package saps.catalog.core.retry.catalog;

import saps.common.core.model.SapsImage;

import saps.catalog.core.Catalog;

public class GetTaskById implements CatalogRetry<SapsImage> {

	private Catalog imageStore;
	private String taskId;

	public GetTaskById(Catalog imageStore, String taskId) {
		this.imageStore = imageStore;
		this.taskId = taskId;
	}

	@Override
	public SapsImage run() {
		return imageStore.getTaskById(taskId);
	}

}
