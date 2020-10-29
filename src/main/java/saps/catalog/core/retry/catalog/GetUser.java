package saps.catalog.core.retry.catalog;

import org.apache.log4j.Logger;
import saps.catalog.core.exceptions.CatalogException;
import saps.catalog.core.exceptions.UserNotFoundException;
import saps.common.core.model.SapsUser;

import saps.catalog.core.Catalog;

public class GetUser implements CatalogRetry<SapsUser> {

	private Catalog imageStore;
	private String userEmail;
	public static final Logger LOGGER = Logger.getLogger(GetUser.class);

	public GetUser(Catalog imageStore, String userEmail) {
		this.imageStore = imageStore;
		this.userEmail = userEmail;
	}

	@Override
	public SapsUser run() {
		try {
			return imageStore.getUserByEmail(userEmail);
		} catch (CatalogException | UserNotFoundException e) {
			LOGGER.error("Error while gets user by email.", e);
		}
		return null;
	}

}
