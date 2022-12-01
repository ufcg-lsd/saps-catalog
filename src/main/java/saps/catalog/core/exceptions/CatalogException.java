/* (C)2020 */
package saps.catalog.core.exceptions;

public class CatalogException extends RuntimeException {

  private static final long serialVersionUID = -2520888793776997437L;

  public CatalogException(String msg) {
    super(msg);
  }

  public CatalogException(String msg, Exception e) {
    super(msg, e);
  }

}
