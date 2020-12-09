package org.greenplum.pxf.service.rest;

import org.greenplum.pxf.api.model.QuerySession;
import org.springframework.http.ResponseEntity;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

@RestController
@RequestMapping("/pxf/" + Version.PXF_PROTOCOL_VERSION)
public class ReadController {

    @GetMapping("/read")
    public ResponseEntity<StreamingResponseBody> read(
            @RequestHeader MultiValueMap<String, String> headers) {

        QuerySession querySession;
        
        // Get the query session
        // QuerySession has the processor, the RequestContext, etc
        
        // register the query session to the query manager, where the query
        // manager will schedule work for the given query session.
        // queryManager.register(querySession, segmentId);
        
        return new ReadResponse(querySession);
    }

}