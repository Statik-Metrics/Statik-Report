package com.trendrr.beanstalk;

/**
 * @author Dustin Norlander
 * @created Nov 15, 2010
 */
public class BeanstalkDisconnectedException extends BeanstalkException {

    private static final long serialVersionUID = -312913078892072313L;

    public BeanstalkDisconnectedException() {
        this(null, null);
    }

    public BeanstalkDisconnectedException(String message) {
        this(message, null);
    }

    public BeanstalkDisconnectedException(String message, Exception cause) {
        super(message, cause);
    }

    public BeanstalkDisconnectedException(Exception cause) {
        this(null, cause);
    }
}
