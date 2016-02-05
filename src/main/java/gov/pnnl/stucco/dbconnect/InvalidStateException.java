package gov.pnnl.stucco.dbconnect;

public class InvalidStateException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 8420550736297816832L;

	public InvalidStateException() {
	}

	public InvalidStateException(String message) {
		super(message);
	}

	public InvalidStateException(Throwable cause) {
		super(cause);
	}

	public InvalidStateException(String message, Throwable cause) {
		super(message, cause);
	}

}
