package gov.pnnl.stucco.dbconnect;

public class StuccoDBException extends RuntimeException {

	/**
	 * This class is to be used when exception come from the DB we are using
	 */
	private static final long serialVersionUID = 8420550736297816832L;

	public StuccoDBException() {
	}

	public StuccoDBException(String message) {
		super(message);
	}

	public StuccoDBException(Throwable cause) {
		super(cause);
	}

	public StuccoDBException(String message, Throwable cause) {
		super(message, cause);
	}

}
