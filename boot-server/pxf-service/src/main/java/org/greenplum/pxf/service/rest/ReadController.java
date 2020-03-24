package org.greenplum.pxf.service.rest;

import org.apache.hadoop.conf.Configuration;
import org.greenplum.pxf.api.factory.ConfigurationFactory;
import org.greenplum.pxf.api.factory.ProcessorFactory;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.service.RequestParser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import java.util.Map;

@RestController
@RequestMapping("/pxf/v15")
public class ReadController extends BaseController {

    private final ProcessorFactory processorFactory;

    private final ConfigurationFactory configurationFactory;

    @Autowired
    public ReadController(RequestParser<Map<String, String>> parser,
                          ProcessorFactory processorFactory,
                          ConfigurationFactory configurationFactory) {
        super(parser);
        this.processorFactory = processorFactory;
        this.configurationFactory = configurationFactory;
    }

    @GetMapping("/read")
    public ResponseEntity<StreamingResponseBody> read(@RequestHeader Map<String, String> headers) {
        RequestContext context = parseRequest(headers);
        Configuration configuration = configurationFactory.initConfiguration(
            context.getConfig(),
            context.getServerName(),
            context.getUser(),
            context.getAdditionalConfigProps());

        return new ResponseEntity<>(processorFactory.getPlugin(context, configuration), HttpStatus.OK);
    }
}
