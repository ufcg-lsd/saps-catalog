/* (C)2020 */
package saps.catalog.core.retry.catalog;

import saps.catalog.core.retry.catalog.exceptions.CatalogRetryException;

public interface CatalogRetry<T> {

  T run() throws CatalogRetryException;
}
