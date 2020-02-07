package org.greenplum.pxf.api.serializer.converter;

public interface ValueConverter<Source, Target> {

    Target convert(Source source);

}