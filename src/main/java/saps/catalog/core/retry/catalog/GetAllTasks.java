package saps.catalog.core.retry.catalog;

import java.util.List;

import saps.common.core.model.SapsImage;

import saps.catalog.core.Catalog;

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
