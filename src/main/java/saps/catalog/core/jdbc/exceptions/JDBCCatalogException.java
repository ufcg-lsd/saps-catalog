/* (C)2020 */
package saps.catalog.core.jdbc.exceptions;

public class JDBCCatalogException extends Exception {

  private static final long serialVersionUID = -2520888793776997437L;

  public JDBCCatalogException(String msg, Exception e) {
    super(msg, e);
  }
}
