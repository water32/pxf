package org.greenplum.pxf.service.rest;

import org.greenplum.pxf.api.model.QuerySession;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.service.QuerySessionService;
import org.greenplum.pxf.service.RequestParser;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import static org.greenplum.pxf.api.model.RequestContext.RequestType.SCAN_CONTROLLER;

/**
 * This controller handles requests for scans on external systems. The external
 * scan will produce tuples that will then be serialized to the output stream
 * so it can then be processed by the client.
 */
@RestController
@RequestMapping("/pxf/" + Version.PXF_PROTOCOL_VERSION)
public class ScanController {

    private final RequestParser<MultiValueMap<String, String>> parser;
    private final QuerySessionService<?> querySessionService;

    public ScanController(RequestParser<MultiValueMap<String, String>> parser,
                          QuerySessionService<?> querySessionService) {
        this.parser = parser;
        this.querySessionService = querySessionService;
    }

    @GetMapping("/scan")
    public ResponseEntity<StreamingResponseBody> scan(
            @RequestHeader MultiValueMap<String, String> headers)
            throws Throwable {

        // TODO: should we do minimal parsing? we only need server name, xid,
        //       resource, filter string and segment ID. Minimal parsing can
        //       improve the query performance marginally
        final RequestContext context = parser.parseRequest(headers, SCAN_CONTROLLER);

        // Get the query session
        // QuerySession has the processor, the RequestContext, state of the
        // query, among other information
        QuerySession<?> querySession = querySessionService.get(context);

        if (querySession == null) {
            return new ResponseEntity<>(HttpStatus.OK);
        }

        // Create a streaming class which will consume tuples from the
        // querySession object and serialize them to the output stream
        StreamingResponseBody response = new ScanResponse<>(context.getSegmentId(), querySession);

        // returns the response to the client
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

}