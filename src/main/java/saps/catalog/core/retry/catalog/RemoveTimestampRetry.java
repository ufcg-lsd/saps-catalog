package saps.catalog.core.retry.catalog;

import saps.common.core.model.SapsImage;

import saps.catalog.core.Catalog;

public class RemoveTimestampRetry implements CatalogRetry<Void> {
	private Catalog imageStore;
	private SapsImage task;

	public RemoveTimestampRetry(Catalog imageStore, SapsImage task) {
		this.imageStore = imageStore;
		this.task = task;
	}

	@Override
	public Void run(){
		imageStore.removeStateChangeTime(task.getTaskId(), task.getState(), task.getUpdateTime());
		return null;
	}
}
