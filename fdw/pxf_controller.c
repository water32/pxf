/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

/*
 * pxf_controller.c
 *		  Functions for reading and writing data from the PXF Server.
 *
 * IDENTIFICATION
 *		  contrib/pxf_fdw/pxf_controller.c
 */
#include "pxf_controller.h"

#include "pxf_header.h"
#include "cdb/cdbtm.h"
#include "cdb/cdbvars.h"

/* helper function declarations */
static void BuildUriForRead(PxfFdwScanState *pxfsstate);
static void BuildUriForWrite(PxfFdwModifyState *pxfmstate);
#if PG_VERSION_NUM >= 90600
static size_t FillBuffer(PxfFdwScanState *pxfsstate, char *start, int minlen, int maxlen);
#else
static size_t FillBuffer(PxfFdwScanState *pxfsstate, char *start, size_t size);
#endif

/*
 * Clean up churl related data structures from the PXF FDW modify state.
 */
void
PxfControllerCleanup(PxfFdwModifyState *pxfmstate)
{
	if (pxfmstate == NULL)
		return;

	PxfCurlCleanup(pxfmstate->curl_handle, false);
	pxfmstate->curl_handle = NULL;

	PxfCurlHeadersCleanup(pxfmstate->curl_headers);
	pxfmstate->curl_headers = NULL;

	if (pxfmstate->uri.data)
	{
		pfree(pxfmstate->uri.data);
	}

	if (pxfmstate->options)
	{
		pfree(pxfmstate->options);
	}
}

/*
 * Sets up data before starting import
 */
void
PxfControllerImportStart(PxfFdwScanState *pxfsstate)
{
	pxfsstate->curl_headers = PxfCurlHeadersInit();

	BuildUriForRead(pxfsstate);
	BuildHttpHeaders(pxfsstate->curl_headers,
					 pxfsstate->options,
					 pxfsstate->relation,
					 pxfsstate->filter_str,
					 pxfsstate->retrieved_attrs);

	pxfsstate->curl_handle = PxfCurlInitDownload(pxfsstate->uri.data, pxfsstate->curl_headers, pxfsstate->options->ssl_options);

	/* read some bytes to make sure the connection is established */
	PxfCurlReadCheckConnectivity(pxfsstate->curl_handle);
}

/*
 * Sets up data before starting export
 */
void
PxfControllerExportStart(PxfFdwModifyState *pxfmstate)
{
	BuildUriForWrite(pxfmstate);
	pxfmstate->curl_headers = PxfCurlHeadersInit();
	BuildHttpHeaders(pxfmstate->curl_headers,
					 pxfmstate->options,
					 pxfmstate->relation,
					 NULL,
					 NULL);
	pxfmstate->curl_handle = PxfCurlInitUpload(pxfmstate->uri.data, pxfmstate->curl_headers, pxfmstate->options->ssl_options);
}

/*
 * Reads data from the PXF server into the given buffer of a given size
 */
int
#if PG_VERSION_NUM >= 90600
PxfControllerRead(void *outbuf, int minlen, int maxlen, void *extra)
#else
PxfControllerRead(void *outbuf, int datasize, void *extra)
#endif
{
	PxfFdwScanState *pxfsstate = (PxfFdwScanState *) extra;
#if PG_VERSION_NUM >= 90600
	return (int) FillBuffer(pxfsstate, outbuf, minlen, maxlen);
#else
	return (int) FillBuffer(pxfsstate, outbuf, datasize);
#endif
}

/*
 * Writes data from the given buffer of a given size to the PXF server
 */
int
PxfControllerWrite(PxfFdwModifyState *pxfmstate, char *databuf, int datalen)
{
	size_t		n = 0;

	if (datalen > 0)
	{
		n = PxfCurlWrite(pxfmstate->curl_handle, databuf, datalen);
		elog(DEBUG5, "pxf PxfControllerWrite: segment %d wrote %zu bytes to %s", PXF_SEGMENT_ID, n, pxfmstate->options->resource);
	}

	return (int) n;
}

/*
 * Format the URI for reading by adding PXF service endpoint details
 */
static void
BuildUriForRead(PxfFdwScanState *pxfsstate)
{
	PxfOptions *options = pxfsstate->options;
	Assert(options->pxf_protocol != NULL && options->pxf_host != NULL && options->pxf_port > 0);

	resetStringInfo(&pxfsstate->uri);
	appendStringInfo(&pxfsstate->uri, "%s://%s:%d/%s/%s/read", options->pxf_protocol, options->pxf_host, options->pxf_port, PXF_SERVICE_PREFIX, PXF_VERSION);
	elog(DEBUG2, "pxf_fdw: uri %s for read", pxfsstate->uri.data);
}

/*
 * Format the URI for writing by adding PXF service endpoint details
 */
static void
BuildUriForWrite(PxfFdwModifyState *pxfmstate)
{
	PxfOptions *options = pxfmstate->options;

	resetStringInfo(&pxfmstate->uri);
	appendStringInfo(&pxfmstate->uri, "%s://%s/%s/%s/Writable/stream", options->pxf_protocol, psprintf("%s:%d", options->pxf_host, options->pxf_port), PXF_SERVICE_PREFIX, PXF_VERSION);
	elog(DEBUG2, "pxf_fdw: uri %s with file name for write: %s", pxfmstate->uri.data, options->resource);
}

/*
 * Read data from churl until the buffer is full or there is no more data to be read
 */
static size_t
#if PG_VERSION_NUM >= 90600
FillBuffer(PxfFdwScanState *pxfsstate, char *start, int minlen, int maxlen)
#else
FillBuffer(PxfFdwScanState *pxfsstate, char *start, size_t size)
#endif
{
	size_t		n = 0;
	char	   *ptr = start;
#if PG_VERSION_NUM >= 90600
	char	   *minend = ptr + minlen;
	char	   *maxend = ptr + maxlen;

	while (ptr < minend)
	{
		n = PxfCurlRead(pxfsstate->curl_handle, ptr, maxend - ptr);
#else
		char	   *end = ptr + size;

	while (ptr < end)
	{
		n = PxfCurlRead(pxfsstate->curl_handle, ptr, end - ptr);
#endif
		if (n == 0)
		{
			/*
			 * done processing all data - check if the
			 * connection terminated with an error
			 */
			PxfCurlReadCheckConnectivity(pxfsstate->curl_handle);
			break;
		}

		ptr += n;
	}

	return ptr - start;
}
