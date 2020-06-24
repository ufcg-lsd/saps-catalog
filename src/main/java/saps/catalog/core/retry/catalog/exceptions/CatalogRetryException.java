package saps.catalog.core.retry.catalog.exceptions;

public class CatalogRetryException extends RuntimeException {

    private static final long serialVersionUID = -2520888793776997437L;

    public CatalogRetryException(String msg) {
        super(msg);
    }
}
