package org.greenplum.pxf.service.spring;

import org.greenplum.pxf.api.error.PxfRuntimeException;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;
import org.springframework.web.util.WebUtils;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Handler for PXF specific exceptions that just reports the request error status. The actual message body with
 * all proper error attributes is created by the Spring MVC BasicErrorController.
 * <p>
 * This handler prevents the PXF specific exception from being thrown to the container, where it would've gotten
 * logged without an MDC context, since by that time the MDC context is cleaned up.
 * <p>
 * Instead, it is assumed that the PXF specific exception has been seen by the PXF resource
 * or the processing logic and was logged there, where the MDC context is still available.
 */
@ControllerAdvice
public class PxfExceptionHandler /*extends ResponseEntityExceptionHandler*/ {

    @ExceptionHandler({PxfRuntimeException.class})
    public void handlePxfRuntimeException(PxfRuntimeException e, HttpServletRequest request, HttpServletResponse response) throws IOException {
        if (response.isCommitted()) {
            // streaming has already started, it's too late to send an error
            // re-throw the exception so that Tomcat can write the error response and
            // terminate the connection immediately without writing the end 0-length chunk
            // causing the client to recognize an error occurred
            response.flushBuffer();
            request.setAttribute(WebUtils.ERROR_EXCEPTION_ATTRIBUTE, e);
//            throw e;
//            response.sendError(HttpStatus.INTERNAL_SERVER_ERROR.value());
        } else {
            // do not re-throw the error, just set the error status
            response.sendError(HttpStatus.INTERNAL_SERVER_ERROR.value());
        }
    }
}
