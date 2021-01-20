/*-------------------------------------------------------------------------
 *
 * copy.c
 *		Implements the COPY utility command
 *
 * Portions Copyright (c) 2005-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/copy.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <ctype.h>
#include <unistd.h>

#include "pxf_copy.h"

#include "access/heapam.h"
#include "commands/defrem.h"
#include "executor/execPartition.h"
#include "executor/executor.h"
#include "executor/nodeModifyTable.h"
#include "executor/tuptable.h"
#include "foreign/fdwapi.h"
#include "libpq/libpq.h"
#include "mb/pg_wchar.h"
#include "optimizer/optimizer.h"
#include "parser/parse_coerce.h"
#include "port/pg_bswap.h"
#include "rewrite/rewriteHandler.h"
#include "storage/execute_pipe.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/partcache.h"
#include "utils/rel.h"

#include "access/url.h"
#include "catalog/namespace.h"
#include "cdb/cdbcopy.h"
#include "cdb/cdbdispatchresult.h"
#include "cdb/cdbsreh.h"
#include "cdb/cdbvars.h"
#include "nodes/makefuncs.h"
#include "utils/resscheduler.h"
#include "utils/string_utils.h"


#define ISOCTAL(c) (((c) >= '0') && ((c) <= '7'))
#define OCTVALUE(c) ((c) - '0')


/*
 * These macros centralize code used to process line_buf and raw_buf buffers.
 * They are macros because they often do continue/break control and to avoid
 * function call overhead in tight COPY loops.
 *
 * We must use "if (1)" because the usual "do {...} while(0)" wrapper would
 * prevent the continue/break processing from working.  We end the "if (1)"
 * with "else ((void) 0)" to ensure the "if" does not unintentionally match
 * any "else" in the calling code, and to avoid any compiler warnings about
 * empty statements.  See http://www.cit.gu.edu.au/~anthony/info/C/C.macros.
 */

/*
 * This keeps the character read at the top of the loop in the buffer
 * even if there is more than one read-ahead.
 */
#define IF_NEED_REFILL_AND_NOT_EOF_CONTINUE(extralen) \
if (1) \
{ \
	if (raw_buf_ptr + (extralen) >= copy_buf_len && !hit_eof) \
	{ \
		raw_buf_ptr = prev_raw_ptr; /* undo fetch */ \
		need_data = true; \
		continue; \
	} \
} else ((void) 0)

/* This consumes the remainder of the buffer and breaks */
#define IF_NEED_REFILL_AND_EOF_BREAK(extralen) \
if (1) \
{ \
	if (raw_buf_ptr + (extralen) >= copy_buf_len && hit_eof) \
	{ \
		if (extralen) \
			raw_buf_ptr = copy_buf_len; /* consume the partial character */ \
		/* backslash just before EOF, treat as data char */ \
		result = true; \
		break; \
	} \
} else ((void) 0)

/*
 * Transfer any approved data to line_buf; must do this to be sure
 * there is some room in raw_buf.
 */
#define REFILL_LINEBUF \
if (1) \
{ \
	if (raw_buf_ptr > cstate->raw_buf_index) \
	{ \
		appendBinaryStringInfo(&cstate->line_buf, \
							 cstate->raw_buf + cstate->raw_buf_index, \
							   raw_buf_ptr - cstate->raw_buf_index); \
		cstate->raw_buf_index = raw_buf_ptr; \
	} \
} else ((void) 0)

/* Undo any read-ahead and jump out of the block. */
#define NO_END_OF_COPY_GOTO \
if (1) \
{ \
	raw_buf_ptr = prev_raw_ptr + 1; \
	goto not_end_of_copy; \
} else ((void) 0)

static const char BinarySignature[8] = "PXF\n\377\r\n\0";


/* non-export function prototypes */
static void PxfEndCopy(PxfCopyState cstate);
static bool CopyReadLine(PxfCopyState cstate);
static bool CopyReadLineText(PxfCopyState cstate);
static int	CopyReadAttributesText(PxfCopyState cstate, int stop_processing_at_field);
static int	CopyReadAttributesCSV(PxfCopyState cstate, int stop_processing_at_field);
static Datum CopyReadBinaryAttribute(PxfCopyState cstate,
									 int column_no, FmgrInfo *flinfo,
									 Oid typioparam, int32 typmod,
									 bool *isnull);
static void CopyAttributeOutText(PxfCopyState cstate, char *string);
static void CopyAttributeOutCSV(PxfCopyState cstate, char *string,
								bool use_quote, bool single_attr);

/* Low-level communications functions */
static void CopySendData(PxfCopyState cstate, const void *databuf, int datasize);
static void PxfCopySendString(PxfCopyState cstate, const char *str);
static void PxfCopySendChar(PxfCopyState cstate, char c);
static int	CopyGetData(PxfCopyState cstate, void *databuf, int datasize);
static void CopySendInt32(PxfCopyState cstate, int32 val);
static bool CopyGetInt32(PxfCopyState cstate, int32 *val);
static void CopySendInt16(PxfCopyState cstate, int16 val);
//static bool CopyGetInt16(PxfCopyState cstate, int16 *val);

static void SendCopyFromForwardedError(PxfCopyState cstate, CdbCopy *cdbCopy, char *errormsg);

static bool NextCopyFromRawFieldsX(PxfCopyState cstate, char ***fields, int *nfields,
								   int stop_processing_at_field);
static bool NextCopyFromX(PxfCopyState cstate, ExprContext *econtext,
						  Datum *values, bool *nulls);
static void HandleCopyError(PxfCopyState cstate);

static void setEncodingConversionProc(PxfCopyState cstate, int encoding, bool iswritable);

static List *parse_joined_option_list(char *str, char *delimiter);

/* ==========================================================================
 * The following macros aid in major refactoring of data processing code (in
 * CopyFrom(+Dispatch)). We use macros because in some cases the code must be in
 * line in order to work (for example elog_dismiss() in PG_CATCH) while in
 * other cases we'd like to inline the code for performance reasons.
 *
 * NOTE that an almost identical set of macros exists in fileam.c. If you make
 * changes here you may want to consider taking a look there as well.
 * ==========================================================================
 */

typedef struct
{
	int64		error_marker;	/* constant -1, to mark that this is an error
								 * frame rather than 'copy_from_dispatch_row' */
	int64		lineno;
	uint32		errmsg_len;
	uint32		line_len;
	bool		line_buf_converted;

	/* 'errmsg' follows */
	/* 'line' follows */
} copy_from_dispatch_error;

/* Size of the struct, without padding at the end. */
#define SizeOfCopyFromDispatchError (offsetof(copy_from_dispatch_error, line_buf_converted) + sizeof(bool))

/*----------
 * CopySendData sends output data to the destination (file or frontend)
 * CopySendString does the same for null-terminated strings
 * CopySendChar does the same for single characters
 * CopySendEndOfRow does the appropriate thing at end of each data row
 *	(data is not actually flushed except by CopySendEndOfRow)
 *
 * NB: no data conversion is applied by these functions
 *----------
 */
static void
CopySendData(PxfCopyState cstate, const void *databuf, int datasize)
{
	appendBinaryStringInfo(cstate->fe_msgbuf, databuf, datasize);
}

static void
PxfCopySendString(PxfCopyState cstate, const char *str)
{
	appendBinaryStringInfo(cstate->fe_msgbuf, str, strlen(str));
}

static void
PxfCopySendChar(PxfCopyState cstate, char c)
{
	appendStringInfoCharMacro(cstate->fe_msgbuf, c);
}

/* AXG: Note that this will both add a newline AND flush the data.
 * For the dispatcher COPY TO we don't want to use this method since
 * our newlines already exist. We use another new method similar to
 * this one to flush the data
 */
void
PxfCopySendEndOfRow(PxfCopyState cstate)
{
	/* we don't actually do the write here, we let the caller do it */
#ifndef WIN32
	PxfCopySendChar(cstate, '\n');
#else
	PxfCopySendString(cstate, "\r\n");
#endif
}

/*
 * CopyGetData reads data from the source (file or frontend)
 *
 * Note: when copying from the frontend, we expect a proper EOF mark per
 * protocol; if the frontend simply drops the connection, we raise error.
 * It seems unwise to allow the COPY IN to complete normally in that case.
 *
 * NB: no data conversion is applied here.
 *
 * Returns: the number of bytes that were successfully read
 * into the data buffer.
 */
static int
CopyGetData(PxfCopyState cstate, void *databuf, int datasize)
{
	size_t		bytesread;

	bytesread = cstate->data_source_cb(databuf, datasize, datasize,
									   cstate->data_source_cb_extra);

	return bytesread;
}


/*
 * These functions do apply some data conversion
 */

/*
 * CopySendInt32 sends an int32 in network byte order
 */
static void
CopySendInt32(PxfCopyState cstate, int32 val)
{
	uint32		buf;

	buf = pg_hton32((uint32) val);
	CopySendData(cstate, &buf, sizeof(buf));
}

/*
 * CopyGetInt32 reads an int32 that appears in network byte order
 *
 * Returns true if OK, false if EOF
 */
static bool
CopyGetInt32(PxfCopyState cstate, int32 *val)
{
	uint32		buf;

	if (CopyGetData(cstate, &buf, sizeof(buf)) != sizeof(buf))
	{
		*val = 0;				/* suppress compiler warning */
		return false;
	}
	*val = (int32) pg_ntoh32(buf);
	return true;
}

/*
 * CopySendInt16 sends an int16 in network byte order
 */
static void
CopySendInt16(PxfCopyState cstate, int16 val)
{
	uint16		buf;

	buf = pg_hton16((uint16) val);
	CopySendData(cstate, &buf, sizeof(buf));
}

///*
// * CopyGetInt16 reads an int16 that appears in network byte order
// */
//static bool
//CopyGetInt16(PxfCopyState cstate, int16 *val)
//{
//	uint16		buf;
//
//	if (CopyGetData(cstate, &buf, sizeof(buf)) != sizeof(buf))
//	{
//		*val = 0;				/* suppress compiler warning */
//		return false;
//	}
//	*val = (int16) pg_ntoh16(buf);
//	return true;
//}


/*
 * CopyLoadRawBuf loads some more data into raw_buf
 *
 * Returns true if able to obtain at least one more byte, else false.
 *
 * If raw_buf_index < raw_buf_len, the unprocessed bytes are transferred
 * down to the start of the buffer and then we load more data after that.
 * This case is used only when a frontend multibyte character crosses a
 * bufferload boundary.
 */
static bool
CopyLoadRawBuf(PxfCopyState cstate)
{
	int			nbytes;
	int			inbytes;

	if (cstate->raw_buf_index < cstate->raw_buf_len)
	{
		/* Copy down the unprocessed data */
		nbytes = cstate->raw_buf_len - cstate->raw_buf_index;
		memmove(cstate->raw_buf, cstate->raw_buf + cstate->raw_buf_index,
				nbytes);
	}
	else
		nbytes = 0;				/* no data need be saved */

	inbytes = CopyGetData(cstate, cstate->raw_buf + nbytes,
						  RAW_BUF_SIZE - nbytes);
	nbytes += inbytes;
	cstate->raw_buf[nbytes] = '\0';
	cstate->raw_buf_index = 0;
	cstate->raw_buf_len = nbytes;
	return (inbytes > 0);
}

/*
 * Process the statement option list for COPY.
 *
 * Scan the options list (a list of DefElem) and transpose the information
 * into cstate, applying appropriate error checking.
 *
 * cstate is assumed to be filled with zeroes initially.
 *
 * This is exported so that external users of the COPY API can sanity-check
 * a list of options.  In that usage, cstate should be passed as NULL
 * (since external users don't know sizeof(CopyStateData)) and the collected
 * data is just leaked until CurrentMemoryContext is reset.
 *
 * Note that additional checking, such as whether column names listed in FORCE
 * QUOTE actually exist, has to be applied later.  This just checks for
 * self-consistency of the options list.
 */
void
PxfProcessCopyOptions(ParseState *pstate,
				   PxfCopyState cstate,
				   bool is_from,
				   List *options)
{
	bool		format_specified = false;
	ListCell   *option;

	/* Support external use for option sanity checking */
	if (cstate == NULL)
		cstate = (PxfCopyStateData *) palloc0(sizeof(PxfCopyStateData));

	cstate->escape_off = false;
	cstate->skip_ext_partition = false;

	cstate->is_copy_from = is_from;

	cstate->delim_off = false;
	cstate->file_encoding = -1;

	/* Extract options from the statement node tree */
	foreach(option, options)
	{
		DefElem    *defel = lfirst_node(DefElem, option);

		if (strcmp(defel->defname, "format") == 0)
		{
			char	   *fmt = defGetString(defel);

			if (format_specified)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
						 parser_errposition(pstate, defel->location)));
			format_specified = true;
			if (strcmp(fmt, "text") == 0)
				 /* default format */ ;
			else if (strcmp(fmt, "csv") == 0)
				cstate->csv_mode = true;
			else if (strcmp(fmt, "binary") == 0)
				cstate->binary = true;
			else
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("COPY format \"%s\" not recognized", fmt),
						 parser_errposition(pstate, defel->location)));
		}
		else if (strcmp(defel->defname, "freeze") == 0)
		{
			if (cstate->freeze)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
						 parser_errposition(pstate, defel->location)));
			cstate->freeze = defGetBoolean(defel);
		}
		else if (strcmp(defel->defname, "delimiter") == 0)
		{
			if (cstate->delim)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
						 parser_errposition(pstate, defel->location)));
			cstate->delim = defGetString(defel);

			if (cstate->delim && pg_strcasecmp(cstate->delim, "off") == 0)
				cstate->delim_off = true;
		}
		else if (strcmp(defel->defname, "null") == 0)
		{
			if (cstate->null_print)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
						 parser_errposition(pstate, defel->location)));
			cstate->null_print = defGetString(defel);

			/*
			 * MPP-2010: unfortunately serialization function doesn't
			 * distinguish between 0x0 and empty string. Therefore we
			 * must assume that if NULL AS was indicated and has no value
			 * the actual value is an empty string.
			 */
			if(!cstate->null_print)
				cstate->null_print = "";
		}
		else if (strcmp(defel->defname, "header") == 0)
		{
			if (cstate->header_line)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
						 parser_errposition(pstate, defel->location)));
			cstate->header_line = defGetBoolean(defel);
		}
		else if (strcmp(defel->defname, "quote") == 0)
		{
			if (cstate->quote)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
						 parser_errposition(pstate, defel->location)));
			cstate->quote = defGetString(defel);
		}
		else if (strcmp(defel->defname, "escape") == 0)
		{
			if (cstate->escape)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
						 parser_errposition(pstate, defel->location)));
			cstate->escape = defGetString(defel);
		}
		else if (strcmp(defel->defname, "force_quote") == 0)
		{
			if (cstate->force_quote || cstate->force_quote_all)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
						 parser_errposition(pstate, defel->location)));
			if (defel->arg && IsA(defel->arg, A_Star))
				cstate->force_quote_all = true;
			else if (defel->arg && IsA(defel->arg, List))
				cstate->force_quote = castNode(List, defel->arg);
			else if (defel->arg && IsA(defel->arg, String))
			{
				if (strcmp(strVal(defel->arg), "*") == 0)
					cstate->force_quote_all = true;
				else
				{
					/* OPTIONS (force_quote 'c1,c2') */
					cstate->force_quote = parse_joined_option_list(strVal(defel->arg), ",");
				}
			}
			else
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("argument to option \"%s\" must be a list of column names",
								defel->defname),
						 parser_errposition(pstate, defel->location)));
		}
		else if (strcmp(defel->defname, "force_not_null") == 0)
		{
			if (cstate->force_notnull)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
						 parser_errposition(pstate, defel->location)));
			if (defel->arg && IsA(defel->arg, List))
				cstate->force_notnull = castNode(List, defel->arg);
			else if (defel->arg && IsA(defel->arg, String))
			{
				/* OPTIONS (force_not_null 'c1,c2') */
				cstate->force_notnull = parse_joined_option_list(strVal(defel->arg), ",");
			}
			else
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("argument to option \"%s\" must be a list of column names",
								defel->defname),
						 parser_errposition(pstate, defel->location)));
		}
		else if (strcmp(defel->defname, "force_null") == 0)
		{
			if (cstate->force_null)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			if (defel->arg && IsA(defel->arg, List))
				cstate->force_null = castNode(List, defel->arg);
			else
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("argument to option \"%s\" must be a list of column names",
								defel->defname),
						 parser_errposition(pstate, defel->location)));
		}
		else if (strcmp(defel->defname, "convert_selectively") == 0)
		{
			/*
			 * Undocumented, not-accessible-from-SQL option: convert only the
			 * named columns to binary form, storing the rest as NULLs. It's
			 * allowed for the column list to be NIL.
			 */
			if (cstate->convert_selectively)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
						 parser_errposition(pstate, defel->location)));
			cstate->convert_selectively = true;
			if (defel->arg == NULL || IsA(defel->arg, List))
				cstate->convert_select = castNode(List, defel->arg);
			else
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("argument to option \"%s\" must be a list of column names",
								defel->defname),
						 parser_errposition(pstate, defel->location)));
		}
		else if (strcmp(defel->defname, "encoding") == 0)
		{
			if (cstate->file_encoding >= 0)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options"),
						 parser_errposition(pstate, defel->location)));
			cstate->file_encoding = pg_char_to_encoding(defGetString(defel));
			if (cstate->file_encoding < 0)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("argument to option \"%s\" must be a valid encoding name",
								defel->defname),
						 parser_errposition(pstate, defel->location)));
		}
		else if (strcmp(defel->defname, "fill_missing_fields") == 0)
		{
			if (cstate->fill_missing)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			cstate->fill_missing = defGetBoolean(defel);
		}
		else if (strcmp(defel->defname, "newline") == 0)
		{
			if (cstate->eol_str)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			cstate->eol_str = strVal(defel->arg);
		}
		else if (strcmp(defel->defname, "sreh") == 0)
		{
			if (defel->arg == NULL || !IsA(defel->arg, SingleRowErrorDesc))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("argument to option \"%s\" must be a list of column names",
								defel->defname)));
			if (cstate->sreh)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));

			cstate->sreh = (SingleRowErrorDesc *) defel->arg;
		}
		else if (strcmp(defel->defname, "on_segment") == 0)
		{
			if (cstate->on_segment)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			cstate->on_segment = true;
		}
		else if (strcmp(defel->defname, "skip_ext_partition") == 0)
		{
			if (cstate->skip_ext_partition)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			cstate->skip_ext_partition = true;
		}
		else if (!rel_is_external_table(cstate->rel->rd_id))
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("option \"%s\" not recognized",
							defel->defname),
					 parser_errposition(pstate, defel->location)));
	}

	/*
	 * Check for incompatible options (must do these two before inserting
	 * defaults)
	 */
	if (cstate->binary && cstate->delim)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("COPY cannot specify DELIMITER in BINARY mode")));

	if (cstate->binary && cstate->null_print)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("COPY cannot specify NULL in BINARY mode")));

	cstate->eol_type = PXF_EOL_UNKNOWN;

	/* Set defaults for omitted options */
	if (!cstate->delim)
		cstate->delim = cstate->csv_mode ? "," : "\t";

	if (!cstate->null_print)
		cstate->null_print = cstate->csv_mode ? "" : "\\N";
	cstate->null_print_len = strlen(cstate->null_print);

	if (cstate->csv_mode)
	{
		if (!cstate->quote)
			cstate->quote = "\"";
		if (!cstate->escape)
			cstate->escape = cstate->quote;
	}

	if (!cstate->csv_mode && !cstate->escape)
		cstate->escape = "\\";			/* default escape for text mode */

	/* Only single-byte delimiter strings are supported. */
	/* GPDB: This is checked later */
#if 0
	if (strlen(cstate->delim) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("COPY delimiter must be a single one-byte character")));
#endif

	/* Disallow end-of-line characters */
	if (strchr(cstate->delim, '\r') != NULL ||
		strchr(cstate->delim, '\n') != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("COPY delimiter cannot be newline or carriage return")));

	if (strchr(cstate->null_print, '\r') != NULL ||
		strchr(cstate->null_print, '\n') != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("COPY null representation cannot use newline or carriage return")));

	/*
	 * Disallow unsafe delimiter characters in non-CSV mode.  We can't allow
	 * backslash because it would be ambiguous.  We can't allow the other
	 * cases because data characters matching the delimiter must be
	 * backslashed, and certain backslash combinations are interpreted
	 * non-literally by COPY IN.  Disallowing all lower case ASCII letters is
	 * more than strictly necessary, but seems best for consistency and
	 * future-proofing.  Likewise we disallow all digits though only octal
	 * digits are actually dangerous.
	 */
	if (!cstate->csv_mode && !cstate->delim_off &&
		strchr("\\.abcdefghijklmnopqrstuvwxyz0123456789",
			   cstate->delim[0]) != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("COPY delimiter cannot be \"%s\"", cstate->delim)));

	/* Check header */
	/*
	 * In PostgreSQL, HEADER is not allowed in text mode either, but in GPDB,
	 * only forbid it with BINARY.
	 */
	if (cstate->binary && cstate->header_line)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("COPY cannot specify HEADER in BINARY mode")));

	/* Check quote */
	if (!cstate->csv_mode && cstate->quote != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("COPY quote available only in CSV mode")));

	if (cstate->csv_mode && strlen(cstate->quote) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("COPY quote must be a single one-byte character")));

	if (cstate->csv_mode && cstate->delim[0] == cstate->quote[0] && !cstate->delim_off)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("COPY delimiter and quote must be different")));

	/* Check escape */
	if (cstate->csv_mode && cstate->escape != NULL && strlen(cstate->escape) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("COPY escape in CSV format must be a single character")));

	if (!cstate->csv_mode && cstate->escape != NULL &&
		(strchr(cstate->escape, '\r') != NULL ||
		strchr(cstate->escape, '\n') != NULL))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("COPY escape representation in text format cannot use newline or carriage return")));

	if (!cstate->csv_mode && cstate->escape != NULL && strlen(cstate->escape) != 1)
	{
		if (pg_strcasecmp(cstate->escape, "off") != 0)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("COPY escape must be a single character, or [OFF/off] to disable escapes")));
	}

	/* Check force_quote */
	if (!cstate->csv_mode && (cstate->force_quote || cstate->force_quote_all))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("COPY force quote available only in CSV mode")));
	if ((cstate->force_quote || cstate->force_quote_all) && is_from)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("COPY force quote only available using COPY TO")));

	/* Check force_notnull */
	if (!cstate->csv_mode && cstate->force_notnull != NIL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("COPY force not null available only in CSV mode")));
	if (cstate->force_notnull != NIL && !is_from)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("COPY force not null only available using COPY FROM")));

	/* Check force_null */
	if (!cstate->csv_mode && cstate->force_null != NIL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("COPY force null available only in CSV mode")));

	if (cstate->force_null != NIL && !is_from)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("COPY force null only available using COPY FROM")));

	/* Don't allow the delimiter to appear in the null string. */
	if (strchr(cstate->null_print, cstate->delim[0]) != NULL && !cstate->delim_off)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("COPY delimiter must not appear in the NULL specification")));

	/* Don't allow the CSV quote char to appear in the null string. */
	if (cstate->csv_mode &&
		strchr(cstate->null_print, cstate->quote[0]) != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("CSV quote character must not appear in the NULL specification")));

	/*
	 * DELIMITER
	 *
	 * Only single-byte delimiter strings are supported. In addition, if the
	 * server encoding is a multibyte character encoding we only allow the
	 * delimiter to be an ASCII character (like postgresql. For more info
	 * on this see discussion and comments in MPP-3756).
	 */
	if (pg_database_encoding_max_length() == 1)
	{
		/* single byte encoding such as ascii, latinx and other */
		if (strlen(cstate->delim) != 1 && !cstate->delim_off)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("COPY delimiter must be a single one-byte character, or \'off\'")));
	}
	else
	{
		/* multi byte encoding such as utf8 */
		if ((strlen(cstate->delim) != 1 || IS_HIGHBIT_SET(cstate->delim[0])) && !cstate->delim_off )
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("COPY delimiter must be a single one-byte character, or \'off\'")));
	}

	if (!cstate->csv_mode && strchr(cstate->delim, '\\') != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("COPY delimiter cannot be backslash")));

	if (cstate->fill_missing && !is_from)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				errmsg("fill missing fields only available for data loading, not unloading")));

	/*
	 * NEWLINE
	 */
	if (cstate->eol_str)
	{
		if (!is_from)
		{
			ereport(ERROR,
					(errcode(ERRCODE_GP_FEATURE_NOT_YET),
					errmsg("newline currently available for data loading only, not unloading")));
		}
		else
		{
			if (pg_strcasecmp(cstate->eol_str, "lf") == 0)
				cstate->eol_type = PXF_EOL_NL;
			else if (pg_strcasecmp(cstate->eol_str, "cr") == 0)
				cstate->eol_type = PXF_EOL_CR;
			else if (pg_strcasecmp(cstate->eol_str, "crlf") == 0)
				cstate->eol_type = PXF_EOL_CRNL;
			else
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("invalid value for NEWLINE \"%s\"",
								cstate->eol_str),
						 errhint("Valid options are: 'LF', 'CRLF' and 'CR'.")));
		}
	}

	if (cstate->escape != NULL && pg_strcasecmp(cstate->escape, "off") == 0)
	{
		cstate->escape_off = true;
	}
}

/*
 * Common setup routines used by BeginCopyFrom and BeginCopyTo.
 *
 * Iff <binary>, unload or reload in the binary format, as opposed to the
 * more wasteful but more robust and portable text format.
 *
 * Iff <oids>, unload or reload the format that includes OID information.
 * On input, we accept OIDs whether or not the table has an OID column,
 * but silently drop them if it does not.  On output, we report an error
 * if the user asks for OIDs in a table that has none (not providing an
 * OID column might seem friendlier, but could seriously confuse programs).
 *
 * If in the text format, delimit columns with delimiter <delim> and print
 * NULL values as <null_print>.
 */
PxfCopyState
PxfBeginCopy(ParseState *pstate,
		  bool is_from,
		  Relation rel,
		  Oid queryRelId,
		  List *attnamelist,
		  List *options,
		  TupleDesc tupDesc)
{
	PxfCopyState	cstate;
	int			num_phys_attrs;
	MemoryContext oldcontext;

	/* Allocate workspace and zero all fields */
	cstate = (PxfCopyStateData *) palloc0(sizeof(PxfCopyStateData));

//	glob_cstate = cstate;

	/*
	 * We allocate everything used by a cstate in a new memory context. This
	 * avoids memory leaks during repeated use of COPY in a query.
	 */
	cstate->copycontext = AllocSetContextCreate(CurrentMemoryContext,
												"COPY",
												ALLOCSET_DEFAULT_SIZES);

	oldcontext = MemoryContextSwitchTo(cstate->copycontext);

	/* Greenplum needs this to detect custom protocol */
	if (rel)
		cstate->rel = rel;

	/* Extract options from the statement node tree */
	PxfProcessCopyOptions(pstate, cstate, is_from, options);

	if (cstate->delim_off && !rel_is_external_table(rel->rd_id))
	{
		/*
		 * We don't support delimiter 'off' for COPY because the QD COPY
		 * sometimes internally adds columns to the data that it sends to
		 * the QE COPY modules, and it uses the delimiter for it. There
		 * are ways to work around this but for now it's not important and
		 * we simply don't support it.
		 */
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("using no delimiter is only supported for external tables")));
	}

	/* Process the source/target relation or query */
	if (rel)
	{
		tupDesc = RelationGetDescr(cstate->rel);
	}

	cstate->attnamelist = attnamelist;
	/* Generate or convert list of attributes to process */
	cstate->attnumlist = CopyGetAttnums(tupDesc, cstate->rel, attnamelist);

	num_phys_attrs = tupDesc->natts;

	/* Convert FORCE_QUOTE name list to per-column flags, check validity */
	cstate->force_quote_flags = (bool *) palloc0(num_phys_attrs * sizeof(bool));
	if (cstate->force_quote_all)
	{
		int			i;

		for (i = 0; i < num_phys_attrs; i++)
			cstate->force_quote_flags[i] = true;
	}
	else if (cstate->force_quote)
	{
		List	   *attnums;
		ListCell   *cur;

		attnums = CopyGetAttnums(tupDesc, cstate->rel, cstate->force_quote);

		foreach(cur, attnums)
		{
			int			attnum = lfirst_int(cur);
			Form_pg_attribute attr = TupleDescAttr(tupDesc, attnum - 1);

			if (!list_member_int(cstate->attnumlist, attnum))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
						 errmsg("FORCE_QUOTE column \"%s\" not referenced by COPY",
								NameStr(attr->attname))));
			cstate->force_quote_flags[attnum - 1] = true;
		}
	}

	/* Convert FORCE_NOT_NULL name list to per-column flags, check validity */
	cstate->force_notnull_flags = (bool *) palloc0(num_phys_attrs * sizeof(bool));
	if (cstate->force_notnull)
	{
		List	   *attnums;
		ListCell   *cur;

		attnums = CopyGetAttnums(tupDesc, cstate->rel, cstate->force_notnull);

		foreach(cur, attnums)
		{
			int			attnum = lfirst_int(cur);
			Form_pg_attribute attr = TupleDescAttr(tupDesc, attnum - 1);

			if (!list_member_int(cstate->attnumlist, attnum))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
						 errmsg("FORCE_NOT_NULL column \"%s\" not referenced by COPY",
								NameStr(attr->attname))));
			cstate->force_notnull_flags[attnum - 1] = true;
		}
	}

	/* Convert FORCE_NULL name list to per-column flags, check validity */
	cstate->force_null_flags = (bool *) palloc0(num_phys_attrs * sizeof(bool));
	if (cstate->force_null)
	{
		List	   *attnums;
		ListCell   *cur;

		attnums = CopyGetAttnums(tupDesc, cstate->rel, cstate->force_null);

		foreach(cur, attnums)
		{
			int			attnum = lfirst_int(cur);
			Form_pg_attribute attr = TupleDescAttr(tupDesc, attnum - 1);

			if (!list_member_int(cstate->attnumlist, attnum))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
						 errmsg("FORCE_NULL column \"%s\" not referenced by COPY",
								NameStr(attr->attname))));
			cstate->force_null_flags[attnum - 1] = true;
		}
	}

	/* Convert convert_selectively name list to per-column flags */
	if (cstate->convert_selectively)
	{
		List	   *attnums;
		ListCell   *cur;

		cstate->convert_select_flags = (bool *) palloc0(num_phys_attrs * sizeof(bool));

		attnums = CopyGetAttnums(tupDesc, cstate->rel, cstate->convert_select);

		foreach(cur, attnums)
		{
			int			attnum = lfirst_int(cur);
			Form_pg_attribute attr = TupleDescAttr(tupDesc, attnum - 1);

			if (!list_member_int(cstate->attnumlist, attnum))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
						 errmsg_internal("selected column \"%s\" not referenced by COPY",
										 NameStr(attr->attname))));
			cstate->convert_select_flags[attnum - 1] = true;
		}
	}

	/* Use client encoding when ENCODING option is not specified. */
	if (cstate->file_encoding < 0)
		cstate->file_encoding = pg_get_client_encoding();

//	/*
//	 * Set up encoding conversion info.  Even if the file and server encodings
//	 * are the same, we must apply pg_any_to_server() to validate data in
//	 * multibyte encodings.
//	 *
//	 * In COPY_EXECUTE mode, the dispatcher has already done the conversion.
//	 */
//	if (cstate->dispatch_mode != COPY_DISPATCH)
//	{
	cstate->need_transcoding =
		((cstate->file_encoding != GetDatabaseEncoding() ||
		  pg_database_encoding_max_length() > 1));
	/* See Multibyte encoding comment above */
	cstate->encoding_embeds_ascii = PG_ENCODING_IS_CLIENT_ONLY(cstate->file_encoding);
	setEncodingConversionProc(cstate, cstate->file_encoding, !is_from);
//	}
//	else
//	{
//		cstate->need_transcoding = false;
//		cstate->encoding_embeds_ascii = PG_ENCODING_IS_CLIENT_ONLY(cstate->file_encoding);
//	}

	MemoryContextSwitchTo(oldcontext);

	return cstate;
}

/*
 * Release resources allocated in a cstate for COPY TO/FROM.
 */
static void
PxfEndCopy(PxfCopyState cstate)
{
//	if (cstate->is_program)
//	{
//		close_program_pipes(cstate, true);
//	}
//	else
//	{
//	if (cstate->filename != NULL && FreeFile(cstate->copy_file))
//		ereport(ERROR,
//				(errcode_for_file_access(),
//				 errmsg("could not close file \"%s\": %m",
//						cstate->filename)));
//	}

	/* Clean up single row error handling related memory */
	if (cstate->cdbsreh)
		destroyCdbSreh(cstate->cdbsreh);

	MemoryContextDelete(cstate->copycontext);
	pfree(cstate);
}

/*
 * Set up CopyState for writing to a foreign or external table.
 */
PxfCopyState
PxfBeginCopyToForeignTable(Relation forrel, List *options)
{
	PxfCopyState	cstate;

	Assert(forrel->rd_rel->relkind == RELKIND_FOREIGN_TABLE);

	cstate = PxfBeginCopy(NULL, false, forrel,
					   InvalidOid,
					   NIL, options,
					   RelationGetDescr(forrel));
//	cstate->dispatch_mode = COPY_DIRECT;
//
//	/*
//	 * We use COPY_CALLBACK to mean that the each line should be
//	 * left in fe_msgbuf. There is no actual callback!
//	 */
//	cstate->copy_dest = COPY_CALLBACK;

	/*
	 * Some more initialization, that in the normal COPY TO codepath, is done
	 * in CopyTo() itself.
	 */
	cstate->null_print_client = cstate->null_print;		/* default */
	if (cstate->need_transcoding)
		cstate->null_print_client = pg_server_to_custom(cstate->null_print,
														cstate->null_print_len,
														cstate->file_encoding,
														cstate->enc_conversion_proc);

	return cstate;
}

/*
 * Emit one row during CopyTo().
 */
void
PxfCopyOneRowTo(PxfCopyState cstate, TupleTableSlot *slot)
{
	bool		need_delim = false;
	FmgrInfo   *out_functions = cstate->out_functions;
	MemoryContext oldcontext;
	ListCell   *cur;
	char	   *string;

	MemoryContextReset(cstate->rowcontext);
	oldcontext = MemoryContextSwitchTo(cstate->rowcontext);

	if (cstate->binary)
	{
		/* Binary per-tuple header */
		CopySendInt16(cstate, list_length(cstate->attnumlist));
	}

	/* Make sure the tuple is fully deconstructed */
	slot_getallattrs(slot);

	foreach(cur, cstate->attnumlist)
	{
		int			attnum = lfirst_int(cur);
		Datum		value = slot->tts_values[attnum - 1];
		bool		isnull = slot->tts_isnull[attnum - 1];

		if (!cstate->binary)
		{
			if (need_delim)
				PxfCopySendChar(cstate, cstate->delim[0]);
			need_delim = true;
		}

		if (isnull)
		{
			if (!cstate->binary)
				PxfCopySendString(cstate, cstate->null_print_client);
			else
				CopySendInt32(cstate, -1);
		}
		else
		{
			if (!cstate->binary)
			{
				char		quotec = cstate->quote ? cstate->quote[0] : '\0';

				/* int2out or int4out ? */
				if (out_functions[attnum -1].fn_oid == 39 ||  /* int2out or int4out */
					out_functions[attnum -1].fn_oid == 43 )
				{
					char tmp[33];
					/*
					 * The standard postgres way is to call the output function, but that involves one or more pallocs,
					 * and a call to sprintf, followed by a conversion to client charset.
					 * Do a fast conversion to string instead.
					 */

					if (out_functions[attnum -1].fn_oid ==  39)
						pg_itoa(DatumGetInt16(value),tmp);
					else
						pg_ltoa(DatumGetInt32(value),tmp);

					/*
					 * Integers don't need quoting, or transcoding to client char
					 * set. We still quote them if FORCE QUOTE was used, though.
					 */
					if (cstate->force_quote_flags[attnum - 1])
						PxfCopySendChar(cstate, quotec);
					CopySendData(cstate, tmp, strlen(tmp));
					if (cstate->force_quote_flags[attnum - 1])
						PxfCopySendChar(cstate, quotec);
				}
				else if (out_functions[attnum -1].fn_oid == 1702)   /* numeric_out */
				{
					string = OutputFunctionCall(&out_functions[attnum - 1],
												value);
					/*
					 * Numerics don't need quoting, or transcoding to client char
					 * set. We still quote them if FORCE QUOTE was used, though.
					 */
					if (cstate->force_quote_flags[attnum - 1])
						PxfCopySendChar(cstate, quotec);
					CopySendData(cstate, string, strlen(string));
					if (cstate->force_quote_flags[attnum - 1])
						PxfCopySendChar(cstate, quotec);
				}
				else
				{
					string = OutputFunctionCall(&out_functions[attnum - 1],
												value);
					if (cstate->csv_mode)
						CopyAttributeOutCSV(cstate, string,
											cstate->force_quote_flags[attnum - 1],
											list_length(cstate->attnumlist) == 1);
					else
						CopyAttributeOutText(cstate, string);
				}
			}
			else
			{
				bytea	   *outputbytes;

				outputbytes = SendFunctionCall(&out_functions[attnum - 1],
											   value);
				CopySendInt32(cstate, VARSIZE(outputbytes) - VARHDRSZ);
				CopySendData(cstate, VARDATA(outputbytes),
							 VARSIZE(outputbytes) - VARHDRSZ);
			}
		}
	}

	MemoryContextSwitchTo(oldcontext);
}

/*
 * error context callback for COPY FROM
 *
 * The argument for the error context must be CopyState.
 */
void
CopyFromErrorCallback(void *arg)
{
	CopyState	cstate = (CopyState) arg;
	char		curlineno_str[32];

	snprintf(curlineno_str, sizeof(curlineno_str), UINT64_FORMAT,
			 cstate->cur_lineno);

	if (cstate->binary)
	{
		/* can't usefully display the data */
		if (cstate->cur_attname)
			errcontext("COPY %s, line %s, column %s",
					   cstate->cur_relname, curlineno_str,
					   cstate->cur_attname);
		else
			errcontext("COPY %s, line %s",
					   cstate->cur_relname, curlineno_str);
	}
	else
	{
		if (cstate->cur_attname && cstate->cur_attval)
		{
			/* error is relevant to a particular column */
			char	   *attval;

			attval = limit_printout_length(cstate->cur_attval);
			errcontext("COPY %s, line %s, column %s: \"%s\"",
					   cstate->cur_relname, curlineno_str,
					   cstate->cur_attname, attval);
			pfree(attval);
		}
		else if (cstate->cur_attname)
		{
			/* error is relevant to a particular column, value is NULL */
			errcontext("COPY %s, line %s, column %s: null input",
					   cstate->cur_relname, curlineno_str,
					   cstate->cur_attname);
		}
		else
		{
			/*
			 * Error is relevant to a particular line.
			 *
			 * If line_buf still contains the correct line, and it's already
			 * transcoded, print it. If it's still in a foreign encoding, it's
			 * quite likely that the error is precisely a failure to do
			 * encoding conversion (ie, bad data). We dare not try to convert
			 * it, and at present there's no way to regurgitate it without
			 * conversion. So we have to punt and just report the line number.
			 */
			if (cstate->line_buf_valid &&
				(cstate->line_buf_converted || !cstate->need_transcoding))
			{
				char	   *lineval;

				lineval = limit_printout_length(cstate->line_buf.data);
				errcontext("COPY %s, line %s: \"%s\"",
						   cstate->cur_relname, curlineno_str, lineval);
				pfree(lineval);
			}
			else
			{
				/*
				 * Here, the line buffer is still in a foreign encoding,
				 * and indeed it's quite likely that the error is precisely
				 * a failure to do encoding conversion (ie, bad data).	We
				 * dare not try to convert it, and at present there's no way
				 * to regurgitate it without conversion.  So we have to punt
				 * and just report the line number.
				 */
				errcontext("COPY %s, line %s",
						   cstate->cur_relname, curlineno_str);
			}
		}
	}
}

/*
 * Make sure we don't print an unreasonable amount of COPY data in a message.
 *
 * It would seem a lot easier to just use the sprintf "precision" limit to
 * truncate the string.  However, some versions of glibc have a bug/misfeature
 * that vsnprintf will always fail (return -1) if it is asked to truncate
 * a string that contains invalid byte sequences for the current encoding.
 * So, do our own truncation.  We return a pstrdup'd copy of the input.
 */
char *
limit_printout_length(const char *str)
{
#define MAX_COPY_DATA_DISPLAY 100

	int			slen = strlen(str);
	int			len;
	char	   *res;

	/* Fast path if definitely okay */
	if (slen <= MAX_COPY_DATA_DISPLAY)
		return pstrdup(str);

	/* Apply encoding-dependent truncation */
	len = pg_mbcliplen(str, slen, MAX_COPY_DATA_DISPLAY);

	/*
	 * Truncate, and add "..." to show we truncated the input.
	 */
	res = (char *) palloc(len + 4);
	memcpy(res, str, len);
	strcpy(res + len, "...");

	return res;
}

/*
 * Setup to read tuples from a file for COPY FROM.
 *
 * 'rel': Used as a template for the tuples
 * 'filename': Name of server-local file to read
 * 'attnamelist': List of char *, columns to include. NIL selects all cols.
 * 'options': List of DefElem. See copy_opt_item in gram.y for selections.
 *
 * Returns a CopyState, to be passed to NextCopyFrom and related functions.
 */
PxfCopyState
PxfBeginCopyFrom(ParseState *pstate,
			  Relation rel,
			  bool is_program,
			  copy_data_source_cb data_source_cb,
			  void *data_source_cb_extra,
			  List *attnamelist,
			  List *options)
{
	PxfCopyState	cstate;
	TupleDesc	tupDesc;
	AttrNumber	num_phys_attrs,
				num_defaults;
	FmgrInfo   *in_functions;
	Oid		   *typioparams;
	int			attnum;
	Oid			in_func_oid;
	int		   *defmap;
	ExprState **defexprs;
	MemoryContext oldcontext;
	bool		volatile_defexprs;

	cstate = PxfBeginCopy(pstate, true, rel, InvalidOid, attnamelist, options, NULL);
	oldcontext = MemoryContextSwitchTo(cstate->copycontext);

	/* Initialize state variables */
	cstate->reached_eof = false;
	// cstate->eol_type = PXF_EOL_UNKNOWN; /* GPDB: don't overwrite value set in ProcessCopyOptions */
	cstate->cur_relname = RelationGetRelationName(cstate->rel);
	cstate->cur_lineno = 0;
	cstate->cur_attname = NULL;
	cstate->cur_attval = NULL;

	/* Set up variables to avoid per-attribute overhead. */
	initStringInfo(&cstate->attribute_buf);
	initStringInfo(&cstate->line_buf);
	cstate->line_buf_converted = false;
	cstate->raw_buf = (char *) palloc(RAW_BUF_SIZE + 1);
	cstate->raw_buf_index = cstate->raw_buf_len = 0;

	/* Assign range table, we'll need it in CopyFrom. */
	if (pstate)
		cstate->range_table = pstate->p_rtable;

	tupDesc = RelationGetDescr(cstate->rel);
	num_phys_attrs = tupDesc->natts;
	num_defaults = 0;
	volatile_defexprs = false;

	/*
	 * Pick up the required catalog information for each attribute in the
	 * relation, including the input function, the element type (to pass to
	 * the input function), and info about defaults and constraints. (Which
	 * input function we use depends on text/binary format choice.)
	 */
	in_functions = (FmgrInfo *) palloc(num_phys_attrs * sizeof(FmgrInfo));
	typioparams = (Oid *) palloc(num_phys_attrs * sizeof(Oid));
	defmap = (int *) palloc(num_phys_attrs * sizeof(int));
	defexprs = (ExprState **) palloc(num_phys_attrs * sizeof(ExprState *));

	for (attnum = 1; attnum <= num_phys_attrs; attnum++)
	{
		Form_pg_attribute att = TupleDescAttr(tupDesc, attnum - 1);

		/* We don't need info for dropped attributes */
		if (att->attisdropped)
			continue;

		/* Fetch the input function and typioparam info
		 * For numeric, we don't process it in binary format, but rather in string format */
		if (cstate->binary && att->atttypid != 1700)
			getTypeBinaryInputInfo(att->atttypid,
								   &in_func_oid, &typioparams[attnum - 1]);
		else
			getTypeInputInfo(att->atttypid,
							 &in_func_oid, &typioparams[attnum - 1]);
		fmgr_info(in_func_oid, &in_functions[attnum - 1]);

		/* TODO: is force quote array necessary for default conversion */

		/* Get default info if needed */
		if (!list_member_int(cstate->attnumlist, attnum) && !att->attgenerated)
		{
			/* attribute is NOT to be copied from input */
			/* use default value if one exists */
			Expr	   *defexpr = (Expr *) build_column_default(cstate->rel,
																attnum);

			if (defexpr != NULL)
			{
				/* Run the expression through planner */
				defexpr = expression_planner(defexpr);

				/* Initialize executable expression in copycontext */
				defexprs[num_defaults] = ExecInitExpr(defexpr, NULL);
				defmap[num_defaults] = attnum - 1;
				num_defaults++;

				/*
				 * If a default expression looks at the table being loaded,
				 * then it could give the wrong answer when using
				 * multi-insert. Since database access can be dynamic this is
				 * hard to test for exactly, so we use the much wider test of
				 * whether the default expression is volatile. We allow for
				 * the special case of when the default expression is the
				 * nextval() of a sequence which in this specific case is
				 * known to be safe for use with the multi-insert
				 * optimization. Hence we use this special case function
				 * checker rather than the standard check for
				 * contain_volatile_functions().
				 */
				if (!volatile_defexprs)
					volatile_defexprs = contain_volatile_functions_not_nextval((Node *) defexpr);
			}
		}
	}

	/* We keep those variables in cstate. */
	cstate->in_functions = in_functions;
	cstate->typioparams = typioparams;
	cstate->defmap = defmap;
	cstate->defexprs = defexprs;
	cstate->volatile_defexprs = volatile_defexprs;
	cstate->num_defaults = num_defaults;
//	cstate->is_program = is_program;

//	bool		pipe = (filename == NULL || cstate->dispatch_mode == COPY_EXECUTOR);

	if (data_source_cb)
	{
//		cstate->copy_dest = COPY_CALLBACK;
		cstate->data_source_cb = data_source_cb;
		cstate->data_source_cb_extra = data_source_cb_extra;
	}

	if (cstate->on_segment && Gp_role == GP_ROLE_DISPATCH)
	{
		/* nothing to do */
	}
	else if (cstate->binary)
	{
		/* Read and verify binary header */
		char		readSig[11];
		int32		tmp;

		/* Signature */
		if (CopyGetData(cstate, readSig, 8) != 8 ||
			memcmp(readSig, BinarySignature, 8) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
					errmsg("PXF file signature not recognized")));
		/* Flags field */
		if (!CopyGetInt32(cstate, &tmp))
			ereport(ERROR,
					(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
					 errmsg("invalid PXF file header (missing flags)")));
		if ((tmp & (1 << 16)) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
					 errmsg("invalid PXF file header (WITH OIDS)")));
		tmp &= ~(1 << 16);
		if ((tmp >> 16) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
					 errmsg("unrecognized critical flags in PXF file header")));
		/* Header extension length */
		if (!CopyGetInt32(cstate, &tmp) ||
			tmp < 0)
			ereport(ERROR,
					(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
					 errmsg("invalid PXF file header (missing length)")));
		/* Skip extension header, if present */
		while (tmp-- > 0)
		{
			if (CopyGetData(cstate, readSig, 1) != 1)
				ereport(ERROR,
						(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
						 errmsg("invalid PXF file header (wrong length)")));
		}
	}

	/* create workspace for CopyReadAttributes results */
	if (!cstate->binary)
	{
		AttrNumber	attr_count = list_length(cstate->attnumlist);

		cstate->max_fields = attr_count;
		cstate->raw_fields = (char **) palloc(attr_count * sizeof(char *));
	}

	MemoryContextSwitchTo(oldcontext);

	return cstate;
}

static bool
NextCopyFromRawFieldsX(PxfCopyState cstate, char ***fields, int *nfields,
					   int stop_processing_at_field)
{
	int			fldct;
	bool		done;

	/* only available for text or csv input */
	Assert(!cstate->binary);

	/* on input just throw the header line away */
	if (cstate->cur_lineno == 0 && cstate->header_line)
	{
		cstate->cur_lineno++;
		if (CopyReadLine(cstate))
			return false;		/* done */
	}

	cstate->cur_lineno++;

	/* Actually read the line into memory here */
	done = CopyReadLine(cstate);

	/*
	 * EOF at start of line means we're done.  If we see EOF after some
	 * characters, we act as though it was newline followed by EOF, ie,
	 * process the line and then exit loop on next iteration.
	 */
	if (done && cstate->line_buf.len == 0)
		return false;

	/* Parse the line into de-escaped field values */
	if (cstate->csv_mode)
		fldct = CopyReadAttributesCSV(cstate, stop_processing_at_field);
	else
		fldct = CopyReadAttributesText(cstate, stop_processing_at_field);

	*fields = cstate->raw_fields;
	*nfields = fldct;
	return true;
}

bool
PxfNextCopyFrom(PxfCopyState cstate, ExprContext *econtext,
			   Datum *values, bool *nulls)
{
	if (!cstate->cdbsreh)
		return NextCopyFromX(cstate, econtext, values, nulls);
	else
	{
		MemoryContext oldcontext = CurrentMemoryContext;

		for (;;)
		{
			bool		got_error = false;
			bool		result = false;

			PG_TRY();
			{
				result = NextCopyFromX(cstate, econtext, values, nulls);
			}
			PG_CATCH();
			{
				HandleCopyError(cstate); /* cdbsreh->processed is updated inside here */
				got_error = true;
				MemoryContextSwitchTo(oldcontext);
			}
			PG_END_TRY();

			if (result)
				cstate->cdbsreh->processed++;

			if (!got_error)
				return result;
		}
	}
}

/*
 * A data error happened. This code block will always be inside a PG_CATCH()
 * block right when a higher stack level produced an error. We handle the error
 * by checking which error mode is set (SREH or all-or-nothing) and do the right
 * thing accordingly. Note that we MUST have this code in a macro (as opposed
 * to a function) as elog_dismiss() has to be inlined with PG_CATCH in order to
 * access local error state variables.
 *
 * changing me? take a look at FILEAM_HANDLE_ERROR in fileam.c as well.
 */
static void
HandleCopyError(PxfCopyState cstate)
{
	if (cstate->errMode == PXF_ALL_OR_NOTHING)
	{
		/* re-throw error and abort */
		PG_RE_THROW();
	}
	/* SREH must only handle data errors. all other errors must not be caught */
	if (ERRCODE_TO_CATEGORY(elog_geterrcode()) != ERRCODE_DATA_EXCEPTION)
	{
		/* re-throw error and abort */
		PG_RE_THROW();
	}
	else
	{
		/* SREH - release error state and handle error */
		MemoryContext oldcontext;
		ErrorData	*edata;
		char	   *errormsg;
		CdbSreh	   *cdbsreh = cstate->cdbsreh;

		cdbsreh->processed++;

		oldcontext = MemoryContextSwitchTo(cstate->cdbsreh->badrowcontext);

		/* save a copy of the error info */
		edata = CopyErrorData();

		FlushErrorState();

		/*
		 * set the error message. Use original msg and add column name if available.
		 * We do this even if we're not logging the errors, because
		 * ErrorIfRejectLimit() below will use this information in the error message,
		 * if the error count is reached.
		 */
		cdbsreh->rawdata = cstate->line_buf.data;

		cdbsreh->is_server_enc = cstate->line_buf_converted;
		cdbsreh->linenumber = cstate->cur_lineno;
		if (cstate->cur_attname)
		{
			errormsg =  psprintf("%s, column %s",
								 edata->message, cstate->cur_attname);
		}
		else
		{
			errormsg = edata->message;
		}
		cstate->cdbsreh->errmsg = errormsg;

		if (IS_LOG_TO_FILE(cstate->cdbsreh->logerrors))
		{
			if (Gp_role == GP_ROLE_DISPATCH && !cstate->on_segment)
			{
				cstate->cdbsreh->rejectcount++;

				SendCopyFromForwardedError(cstate, cstate->cdbCopy, errormsg);
			}
			else
			{
				/* after all the prep work let cdbsreh do the real work */
				if (Gp_role == GP_ROLE_DISPATCH)
				{
					cstate->cdbsreh->rejectcount++;
				}
				else
				{
					HandleSingleRowError(cstate->cdbsreh);
				}
			}
		}
		else
			cstate->cdbsreh->rejectcount++;

		ErrorIfRejectLimitReached(cstate->cdbsreh);

		MemoryContextSwitchTo(oldcontext);
		MemoryContextReset(cstate->cdbsreh->badrowcontext);
	}
}


/*
 * Read next tuple from file for COPY FROM. Return false if no more tuples.
 *
 * 'econtext' is used to evaluate default expression for each columns not
 * read from the file. It can be NULL when no default values are used, i.e.
 * when all columns are read from the file.
 *
 * 'values' and 'nulls' arrays must be the same length as columns of the
 * relation passed to BeginCopyFrom. This function fills the arrays.
 */
bool
NextCopyFromX(PxfCopyState cstate, ExprContext *econtext,
			 Datum *values, bool *nulls)
{
	TupleDesc	tupDesc;
	AttrNumber	num_phys_attrs,
				attr_count,
				num_defaults = cstate->num_defaults;
	FmgrInfo   *in_functions = cstate->in_functions;
	Oid		   *typioparams = cstate->typioparams;
	int			i;
	int		   *defmap = cstate->defmap;
	ExprState **defexprs = cstate->defexprs;
	List	   *attnumlist;
	int			stop_processing_at_field;

	/*
	 * Figure out what fields we're going to process in this process.
	 *
	 * In the QD, set 'stop_processing_at_field' so that we only those
	 * fields that are needed in the QD.
	 */
//	switch (cstate->dispatch_mode)
//	{
//		case COPY_DIRECT:
			stop_processing_at_field = -1;
			attnumlist = cstate->attnumlist;
//			break;
//
//		case COPY_DISPATCH:
//			stop_processing_at_field = cstate->first_qe_processed_field;
//			attnumlist = cstate->qd_attnumlist;
//			break;
//
//		case COPY_EXECUTOR:
//			stop_processing_at_field = -1;
//			attnumlist = cstate->qe_attnumlist;
//			break;
//
//		default:
//			elog(ERROR, "unexpected COPY dispatch mode %d", cstate->dispatch_mode);
//	}

	tupDesc = RelationGetDescr(cstate->rel);
	num_phys_attrs = tupDesc->natts;
	attr_count = list_length(attnumlist);

	/* Initialize all values for row to NULL */
	MemSet(values, 0, num_phys_attrs * sizeof(Datum));
	MemSet(nulls, true, num_phys_attrs * sizeof(bool));

	if (!cstate->binary)
	{
		char	  **field_strings;
		ListCell   *cur;
		int			fldct;
		int			fieldno;
		char	   *string;

//		/* read raw fields in the next line */
//		if (cstate->dispatch_mode != COPY_EXECUTOR)
//		{
			if (!NextCopyFromRawFieldsX(cstate, &field_strings, &fldct,
										stop_processing_at_field))
				return false;
//		}
//		else
//		{
//			/*
//			 * We have received the raw line from the QD, and we just
//			 * need to split it into raw fields.
//			 */
//			if (cstate->stopped_processing_at_delim &&
//				cstate->line_buf.cursor <= cstate->line_buf.len)
//			{
//				if (cstate->csv_mode)
//					fldct = CopyReadAttributesCSV(cstate, -1);
//				else
//					fldct = CopyReadAttributesText(cstate, -1);
//			}
//			else
//				fldct = 0;
//			field_strings = cstate->raw_fields;
//		}

		/* check for overflowing fields */
		if (attr_count > 0 && fldct > attr_count)
			ereport(ERROR,
					(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
					 errmsg("extra data after last expected column")));

		/*
		 * A completely empty line is not allowed with FILL MISSING FIELDS. Without
		 * FILL MISSING FIELDS, it's almost surely an error, but not always:
		 * a table with a single text column, for example, needs to accept empty
		 * lines.
		 */
		if (cstate->line_buf.len == 0 &&
			cstate->fill_missing &&
			list_length(cstate->attnumlist) > 1)
		{
			ereport(ERROR,
					(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
					 errmsg("missing data for column \"%s\", found empty data line",
							NameStr(TupleDescAttr(tupDesc, 1)->attname))));
		}

		fieldno = 0;

		/* Loop to read the user attributes on the line. */
		foreach(cur, attnumlist)
		{
			int			attnum = lfirst_int(cur);
			int			m = attnum - 1;
			Form_pg_attribute att = TupleDescAttr(tupDesc, m);

			if (fieldno >= fldct)
			{
				/*
				 * Some attributes are missing. In FILL MISSING FIELDS mode,
				 * treat them as NULLs.
				 */
				if (!cstate->fill_missing)
					ereport(ERROR,
						(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
						 errmsg("missing data for column \"%s\"",
								NameStr(att->attname))));
				fieldno++;
				string = NULL;
			}
			else
				string = field_strings[fieldno++];

			if (cstate->convert_select_flags &&
				!cstate->convert_select_flags[m])
			{
				/* ignore input field, leaving column as NULL */
				continue;
			}

			if (cstate->csv_mode)
			{
				if (string == NULL &&
					cstate->force_notnull_flags[m])
				{
					/*
					 * FORCE_NOT_NULL option is set and column is NULL -
					 * convert it to the NULL string.
					 */
					string = cstate->null_print;
				}
				else if (string != NULL && cstate->force_null_flags[m]
						 && strcmp(string, cstate->null_print) == 0)
				{
					/*
					 * FORCE_NULL option is set and column matches the NULL
					 * string. It must have been quoted, or otherwise the
					 * string would already have been set to NULL. Convert it
					 * to NULL as specified.
					 */
					string = NULL;
				}
			}

			cstate->cur_attname = NameStr(att->attname);
			cstate->cur_attval = string;
			values[m] = InputFunctionCall(&in_functions[m],
										  string,
										  typioparams[m],
										  att->atttypmod);
			if (string != NULL)
				nulls[m] = false;
			cstate->cur_attname = NULL;
			cstate->cur_attval = NULL;
		}

		Assert(fieldno == attr_count);
	}
	else if (attr_count)
	{
		/* binary */
		char		readSig[1];
		ListCell   *cur;

		cstate->cur_lineno++;

		if (CopyGetData(cstate, readSig, 1) != 1)
		{
			/* EOF detected (end of file, or protocol-level EOF) */
			return false;
		}

		if (readSig[0] == 0x01)
		{
			/*
			 * Received EOF marker.  In a V3-protocol copy, wait for the
			 * protocol-level EOF, and complain if it doesn't come
			 * immediately.  This ensures that we correctly handle CopyFail,
			 * if client chooses to send that now.
			 *
			 * Note that we MUST NOT try to read more data in an old-protocol
			 * copy, since there is no protocol-level EOF marker then.  We
			 * could go either way for copy from file, but choose to throw
			 * error if there's data after the EOF marker, for consistency
			 * with the new-protocol case.
			 */
			char		dummy;

			if (/*cstate->copy_dest != COPY_OLD_FE &&*/
				CopyGetData(cstate, &dummy, 1) > 0)
				ereport(ERROR,
						(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
						 errmsg("received copy data after EOF marker")));
			return false;
		}
		else if (readSig[0] == 0x03)
		{
			// TODO: handle error
		}

		i = 0;
		foreach(cur, attnumlist)
		{
			int			attnum = lfirst_int(cur);
			int			m = attnum - 1;
			Form_pg_attribute att = TupleDescAttr(tupDesc, m);

			cstate->cur_attname = NameStr(att->attname);
			i++;
			values[m] = CopyReadBinaryAttribute(cstate,
												i,
												&in_functions[m],
												typioparams[m],
												att->atttypmod,
												&nulls[m]);
			cstate->cur_attname = NULL;
		}
	}

	/*
	 * Now compute and insert any defaults available for the columns not
	 * provided by the input data.  Anything not processed here or above will
	 * remain NULL.
	 *
	 * GPDB: The defaults are always computed in the QD, and are included
	 * in the QD->QE stream as pre-computed Datums. Funny indentation, to
	 * keep the indentation of the code inside the same as in upstream.
	 * (We could improve this, and compute immutable defaults that don't
	 * affect which segment the row belongs to, in the QE.)
	 */
//  if (cstate->dispatch_mode != COPY_EXECUTOR)
//  {
	for (i = 0; i < num_defaults; i++)
	{
		/*
		 * The caller must supply econtext and have switched into the
		 * per-tuple memory context in it.
		 */
		Assert(econtext != NULL);
		Assert(CurrentMemoryContext == econtext->ecxt_per_tuple_memory);

		values[defmap[i]] = ExecEvalExpr(defexprs[i], econtext,
										 &nulls[defmap[i]]);
	}
//  }

	return true;
}

static void
SendCopyFromForwardedError(PxfCopyState cstate, CdbCopy *cdbCopy, char *errormsg)
{
	copy_from_dispatch_error *errframe;
	StringInfo	msgbuf;
	int			target_seg;
	int			errormsg_len = strlen(errormsg);

	msgbuf = cstate->dispatch_msgbuf;
	resetStringInfo(msgbuf);
	enlargeStringInfo(msgbuf, SizeOfCopyFromDispatchError);
	/* allocate space for the header (we'll fill it in last). */
	msgbuf->len = SizeOfCopyFromDispatchError;

	appendBinaryStringInfo(msgbuf, errormsg, errormsg_len);
	appendBinaryStringInfo(msgbuf, cstate->line_buf.data, cstate->line_buf.len);

	errframe = (copy_from_dispatch_error *) msgbuf->data;

	errframe->error_marker = -1;
	errframe->lineno = cstate->cur_lineno;
	errframe->line_len = cstate->line_buf.len;
	errframe->errmsg_len = errormsg_len;
	errframe->line_buf_converted = cstate->line_buf_converted;

	/* send the bad data row to a random QE (via roundrobin) */
	if (cstate->lastsegid == cdbCopy->total_segs)
		cstate->lastsegid = 0; /* start over from first segid */

	target_seg = (cstate->lastsegid++ % cdbCopy->total_segs);

	cdbCopySendData(cdbCopy, target_seg, msgbuf->data, msgbuf->len);
}

/*
 * Clean up storage and release resources for COPY FROM.
 */
void
PxfEndCopyFrom(PxfCopyState cstate)
{
	/* No COPY FROM related resources except memory. */

	PxfEndCopy(cstate);
}

/*
 * Read the next input line and stash it in line_buf, with conversion to
 * server encoding.
 *
 * Result is true if read was terminated by EOF, false if terminated
 * by newline.  The terminating newline or EOF marker is not included
 * in the final value of line_buf.
 */
static bool
CopyReadLine(PxfCopyState cstate)
{
	bool		result;

	resetStringInfo(&cstate->line_buf);
	cstate->line_buf_valid = true;

	/* Mark that encoding conversion hasn't occurred yet */
	cstate->line_buf_converted = false;

	/* Parse data and transfer into line_buf */
	result = CopyReadLineText(cstate);

	if (!result)
	{
		/*
		 * If we didn't hit EOF, then we must have transferred the EOL marker
		 * to line_buf along with the data.  Get rid of it.
		 */
		switch (cstate->eol_type)
		{
			case PXF_EOL_NL:
				Assert(cstate->line_buf.len >= 1);
				Assert(cstate->line_buf.data[cstate->line_buf.len - 1] == '\n');
				cstate->line_buf.len--;
				cstate->line_buf.data[cstate->line_buf.len] = '\0';
				break;
			case PXF_EOL_CR:
				Assert(cstate->line_buf.len >= 1);
				Assert(cstate->line_buf.data[cstate->line_buf.len - 1] == '\r');
				cstate->line_buf.len--;
				cstate->line_buf.data[cstate->line_buf.len] = '\0';
				break;
			case PXF_EOL_CRNL:
				Assert(cstate->line_buf.len >= 2);
				Assert(cstate->line_buf.data[cstate->line_buf.len - 2] == '\r');
				Assert(cstate->line_buf.data[cstate->line_buf.len - 1] == '\n');
				cstate->line_buf.len -= 2;
				cstate->line_buf.data[cstate->line_buf.len] = '\0';
				break;
			case PXF_EOL_UNKNOWN:
				/* shouldn't get here */
				Assert(false);
				break;
		}
	}

	/* Done reading the line.  Convert it to server encoding. */
	if (cstate->need_transcoding)
	{
		char	   *cvt;

		cvt = pg_any_to_server(cstate->line_buf.data,
							   cstate->line_buf.len,
							   cstate->file_encoding);
		if (cvt != cstate->line_buf.data)
		{
			/* transfer converted data back to line_buf */
			resetStringInfo(&cstate->line_buf);
			appendBinaryStringInfo(&cstate->line_buf, cvt, strlen(cvt));
			pfree(cvt);
		}
	}

	/* Now it's safe to use the buffer in error messages */
	cstate->line_buf_converted = true;

	return result;
}

/*
 * CopyReadLineText - inner loop of CopyReadLine for text mode
 */
static bool
CopyReadLineText(PxfCopyState cstate)
{
	char	   *copy_raw_buf;
	int			raw_buf_ptr;
	int			copy_buf_len;
	bool		need_data = false;
	bool		hit_eof = false;
	bool		result = false;
	char		mblen_str[2];

	/* CSV variables */
	bool		first_char_in_line = true;
	bool		in_quote = false,
				last_was_esc = false;
	char		quotec = '\0';
	char		escapec = '\0';

	if (cstate->csv_mode)
	{
		quotec = cstate->quote[0];
		escapec = cstate->escape[0];
		/* ignore special escape processing if it's the same as quotec */
		if (quotec == escapec)
			escapec = '\0';
	}

	mblen_str[1] = '\0';

	/*
	 * The objective of this loop is to transfer the entire next input line
	 * into line_buf.  Hence, we only care for detecting newlines (\r and/or
	 * \n) and the end-of-copy marker (\.).
	 *
	 * In CSV mode, \r and \n inside a quoted field are just part of the data
	 * value and are put in line_buf.  We keep just enough state to know if we
	 * are currently in a quoted field or not.
	 *
	 * These four characters, and the CSV escape and quote characters, are
	 * assumed the same in frontend and backend encodings.
	 *
	 * For speed, we try to move data from raw_buf to line_buf in chunks
	 * rather than one character at a time.  raw_buf_ptr points to the next
	 * character to examine; any characters from raw_buf_index to raw_buf_ptr
	 * have been determined to be part of the line, but not yet transferred to
	 * line_buf.
	 *
	 * For a little extra speed within the loop, we copy raw_buf and
	 * raw_buf_len into local variables.
	 */
	copy_raw_buf = cstate->raw_buf;
	raw_buf_ptr = cstate->raw_buf_index;
	copy_buf_len = cstate->raw_buf_len;

	for (;;)
	{
		int			prev_raw_ptr;
		char		c;

		/*
		 * Load more data if needed.  Ideally we would just force four bytes
		 * of read-ahead and avoid the many calls to
		 * IF_NEED_REFILL_AND_NOT_EOF_CONTINUE(), but the COPY_OLD_FE protocol
		 * does not allow us to read too far ahead or we might read into the
		 * next data, so we read-ahead only as far we know we can.  One
		 * optimization would be to read-ahead four byte here if
		 * cstate->copy_dest != COPY_OLD_FE, but it hardly seems worth it,
		 * considering the size of the buffer.
		 */
		if (raw_buf_ptr >= copy_buf_len || need_data)
		{
			REFILL_LINEBUF;

			/*
			 * Try to read some more data.  This will certainly reset
			 * raw_buf_index to zero, and raw_buf_ptr must go with it.
			 */
			if (!CopyLoadRawBuf(cstate))
				hit_eof = true;
			raw_buf_ptr = 0;
			copy_buf_len = cstate->raw_buf_len;

			/*
			 * If we are completely out of data, break out of the loop,
			 * reporting EOF.
			 */
			if (copy_buf_len <= 0)
			{
				result = true;
				break;
			}
			need_data = false;
		}

		/* OK to fetch a character */
		prev_raw_ptr = raw_buf_ptr;
		c = copy_raw_buf[raw_buf_ptr++];

		if (cstate->csv_mode)
		{
			/*
			 * If character is '\\' or '\r', we may need to look ahead below.
			 * Force fetch of the next character if we don't already have it.
			 * We need to do this before changing CSV state, in case one of
			 * these characters is also the quote or escape character.
			 *
			 * Note: old-protocol does not like forced prefetch, but it's OK
			 * here since we cannot validly be at EOF.
			 */
			if (c == '\\' || c == '\r')
			{
				IF_NEED_REFILL_AND_NOT_EOF_CONTINUE(0);
			}

			/*
			 * Dealing with quotes and escapes here is mildly tricky. If the
			 * quote char is also the escape char, there's no problem - we
			 * just use the char as a toggle. If they are different, we need
			 * to ensure that we only take account of an escape inside a
			 * quoted field and immediately preceding a quote char, and not
			 * the second in an escape-escape sequence.
			 */
			if (in_quote && c == escapec)
				last_was_esc = !last_was_esc;
			if (c == quotec && !last_was_esc)
				in_quote = !in_quote;
			if (c != escapec)
				last_was_esc = false;

			/*
			 * Updating the line count for embedded CR and/or LF chars is
			 * necessarily a little fragile - this test is probably about the
			 * best we can do.  (XXX it's arguable whether we should do this
			 * at all --- is cur_lineno a physical or logical count?)
			 */
			if (in_quote && c == (cstate->eol_type == PXF_EOL_NL ? '\n' : '\r'))
				cstate->cur_lineno++;
		}

		/* Process \r */
		if (c == '\r' && (!cstate->csv_mode || !in_quote))
		{
			/* Check for \r\n on first line, _and_ handle \r\n. */
			if (cstate->eol_type == PXF_EOL_UNKNOWN ||
				cstate->eol_type == PXF_EOL_CRNL)
			{
				/*
				 * If need more data, go back to loop top to load it.
				 *
				 * Note that if we are at EOF, c will wind up as '\0' because
				 * of the guaranteed pad of raw_buf.
				 */
				IF_NEED_REFILL_AND_NOT_EOF_CONTINUE(0);

				/* get next char */
				c = copy_raw_buf[raw_buf_ptr];

				if (c == '\n')
				{
					raw_buf_ptr++;	/* eat newline */
					cstate->eol_type = PXF_EOL_CRNL;	/* in case not set yet */

					/*
					 * GPDB: end of line. Since we don't error out if we find a
					 * bare CR or LF in CRLF mode, break here instead.
					 */
					break;
				}
				else
				{
					/*
					 * GPDB_91_MERGE_FIXME: these commented-out blocks (as well
					 * as the restructured newline checks) are here because we
					 * allow the user to manually set the newline mode, and
					 * therefore don't error out on bare CR/LF in the middle of
					 * a column. Instead, they will be included verbatim.
					 *
					 * This probably has other fallout -- but so does changing
					 * the behavior. Discuss.
					 */
#if 0
					/* found \r, but no \n */
					if (cstate->eol_type == PXF_EOL_CRNL)
						ereport(ERROR,
								(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
								 !cstate->csv_mode ?
								 errmsg("literal carriage return found in data") :
								 errmsg("unquoted carriage return found in data"),
								 !cstate->csv_mode ?
								 errhint("Use \"\\r\" to represent carriage return.") :
								 errhint("Use quoted CSV field to represent carriage return.")));
#endif

					/* GPDB: only reset eol_type if it's currently unknown. */
					if (cstate->eol_type == PXF_EOL_UNKNOWN)
					{
						/*
						 * if we got here, it is the first line and we didn't find
						 * \n, so don't consume the peeked character
						 */
						cstate->eol_type = PXF_EOL_CR;
					}
				}
			}
#if 0 /* GPDB_91_MERGE_FIXME: see above. */
			else if (cstate->eol_type == PXF_EOL_NL)
				ereport(ERROR,
						(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
						 !cstate->csv_mode ?
						 errmsg("literal carriage return found in data") :
						 errmsg("unquoted carriage return found in data"),
						 !cstate->csv_mode ?
						 errhint("Use \"\\r\" to represent carriage return.") :
						 errhint("Use quoted CSV field to represent carriage return.")));
#endif
			/* GPDB: a CR only ends the line in CR mode. */
			if (cstate->eol_type == PXF_EOL_CR)
			{
				/* If reach here, we have found the line terminator */
				break;
			}
		}

		/* Process \n */
		if (c == '\n' && (!cstate->csv_mode || !in_quote))
		{
#if 0 /* GPDB_91_MERGE_FIXME: see above. */
			if (cstate->eol_type == PXF_EOL_CR || cstate->eol_type == PXF_EOL_CRNL)
				ereport(ERROR,
						(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
						 !cstate->csv_mode ?
						 errmsg("literal newline found in data") :
						 errmsg("unquoted newline found in data"),
						 !cstate->csv_mode ?
						 errhint("Use \"\\n\" to represent newline.") :
						 errhint("Use quoted CSV field to represent newline.")));
#endif
			/* GPDB: only reset eol_type if it's currently unknown. */
			if (cstate->eol_type == PXF_EOL_UNKNOWN)
				cstate->eol_type = PXF_EOL_NL;	/* in case not set yet */

			/* GPDB: a LF only ends the line in LF mode. */
			if (cstate->eol_type == PXF_EOL_NL)
			{
				/* If reach here, we have found the line terminator */
				break;
			}
		}

		/*
		 * In CSV mode, we only recognize \. alone on a line.  This is because
		 * \. is a valid CSV data value.
		 */
		if (c == '\\' && (!cstate->csv_mode || first_char_in_line))
		{
			char		c2;

			IF_NEED_REFILL_AND_NOT_EOF_CONTINUE(0);
			IF_NEED_REFILL_AND_EOF_BREAK(0);

			/* -----
			 * get next character
			 * Note: we do not change c so if it isn't \., we can fall
			 * through and continue processing for file encoding.
			 * -----
			 */
			c2 = copy_raw_buf[raw_buf_ptr];

			if (c2 == '.')
			{
				raw_buf_ptr++;	/* consume the '.' */

				/*
				 * Note: if we loop back for more data here, it does not
				 * matter that the CSV state change checks are re-executed; we
				 * will come back here with no important state changed.
				 */
				if (cstate->eol_type == PXF_EOL_CRNL)
				{
					/* Get the next character */
					IF_NEED_REFILL_AND_NOT_EOF_CONTINUE(0);
					/* if hit_eof, c2 will become '\0' */
					c2 = copy_raw_buf[raw_buf_ptr++];

					if (c2 == '\n')
					{
						if (!cstate->csv_mode)
						{
							cstate->raw_buf_index = raw_buf_ptr;
							ereport(ERROR,
									(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
									 errmsg("end-of-copy marker does not match previous newline style")));
						}
						else
							NO_END_OF_COPY_GOTO;
					}
					else if (c2 != '\r')
					{
						if (!cstate->csv_mode)
						{
							cstate->raw_buf_index = raw_buf_ptr;
							ereport(ERROR,
									(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
									 errmsg("end-of-copy marker corrupt")));
						}
						else
							NO_END_OF_COPY_GOTO;
					}
				}

				/* Get the next character */
				IF_NEED_REFILL_AND_NOT_EOF_CONTINUE(0);
				/* if hit_eof, c2 will become '\0' */
				c2 = copy_raw_buf[raw_buf_ptr++];

				if (c2 != '\r' && c2 != '\n')
				{
					if (!cstate->csv_mode)
					{
						cstate->raw_buf_index = raw_buf_ptr;
						ereport(ERROR,
								(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
								 errmsg("end-of-copy marker corrupt")));
					}
					else
						NO_END_OF_COPY_GOTO;
				}

				if ((cstate->eol_type == PXF_EOL_NL && c2 != '\n') ||
					(cstate->eol_type == PXF_EOL_CRNL && c2 != '\n') ||
					(cstate->eol_type == PXF_EOL_CR && c2 != '\r'))
				{
					cstate->raw_buf_index = raw_buf_ptr;
					ereport(ERROR,
							(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
							 errmsg("end-of-copy marker does not match previous newline style")));
				}

				/*
				 * Transfer only the data before the \. into line_buf, then
				 * discard the data and the \. sequence.
				 */
				if (prev_raw_ptr > cstate->raw_buf_index)
					appendBinaryStringInfo(&cstate->line_buf,
										   cstate->raw_buf + cstate->raw_buf_index,
										   prev_raw_ptr - cstate->raw_buf_index);
				cstate->raw_buf_index = raw_buf_ptr;
				result = true;	/* report EOF */
				break;
			}
			else if (!cstate->csv_mode)

				/*
				 * If we are here, it means we found a backslash followed by
				 * something other than a period.  In non-CSV mode, anything
				 * after a backslash is special, so we skip over that second
				 * character too.  If we didn't do that \\. would be
				 * considered an eof-of copy, while in non-CSV mode it is a
				 * literal backslash followed by a period.  In CSV mode,
				 * backslashes are not special, so we want to process the
				 * character after the backslash just like a normal character,
				 * so we don't increment in those cases.
				 */
				raw_buf_ptr++;
		}

		/*
		 * This label is for CSV cases where \. appears at the start of a
		 * line, but there is more text after it, meaning it was a data value.
		 * We are more strict for \. in CSV mode because \. could be a data
		 * value, while in non-CSV mode, \. cannot be a data value.
		 */
not_end_of_copy:

		/*
		 * Process all bytes of a multi-byte character as a group.
		 *
		 * We only support multi-byte sequences where the first byte has the
		 * high-bit set, so as an optimization we can avoid this block
		 * entirely if it is not set.
		 */
		if (cstate->encoding_embeds_ascii && IS_HIGHBIT_SET(c))
		{
			int			mblen;

			/*
			 * It is enough to look at the first byte in all our encodings, to
			 * get the length.  (GB18030 is a bit special, but still works for
			 * our purposes; see comment in pg_gb18030_mblen())
			 */
			mblen_str[0] = c;
			mblen = pg_encoding_mblen(cstate->file_encoding, mblen_str);

			IF_NEED_REFILL_AND_NOT_EOF_CONTINUE(mblen - 1);
			IF_NEED_REFILL_AND_EOF_BREAK(mblen - 1);
			raw_buf_ptr += mblen - 1;
		}
		first_char_in_line = false;
	}							/* end of outer loop */

	/*
	 * Transfer any still-uncopied data to line_buf.
	 */
	REFILL_LINEBUF;

	return result;
}

/*
 *	Return decimal value for a hexadecimal digit
 */
static int
GetDecimalFromHex(char hex)
{
	if (isdigit((unsigned char) hex))
		return hex - '0';
	else
		return tolower((unsigned char) hex) - 'a' + 10;
}

/*
 * Parse the current line into separate attributes (fields),
 * performing de-escaping as needed.
 *
 * The input is in line_buf.  We use attribute_buf to hold the result
 * strings.  cstate->raw_fields[k] is set to point to the k'th attribute
 * string, or NULL when the input matches the null marker string.
 * This array is expanded as necessary.
 *
 * (Note that the caller cannot check for nulls since the returned
 * string would be the post-de-escaping equivalent, which may look
 * the same as some valid data string.)
 *
 * delim is the column delimiter string (must be just one byte for now).
 * null_print is the null marker string.  Note that this is compared to
 * the pre-de-escaped input string.
 *
 * The return value is the number of fields actually read.
 */
static int
CopyReadAttributesText(PxfCopyState cstate, int stop_processing_at_field)
{
	char		delimc = cstate->delim[0];
	char		escapec = cstate->escape_off ? delimc : cstate->escape[0];
	bool		delim_off = cstate->delim_off;
	int			fieldno;
	char	   *output_ptr;
	char	   *cur_ptr;
	char	   *line_end_ptr;

	/*
	 * We need a special case for zero-column tables: check that the input
	 * line is empty, and return.
	 */
	if (cstate->max_fields <= 0)
	{
		if (cstate->line_buf.len != 0)
			ereport(ERROR,
					(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
					 errmsg("extra data after last expected column")));
		return 0;
	}

	resetStringInfo(&cstate->attribute_buf);

	/*
	 * The de-escaped attributes will certainly not be longer than the input
	 * data line, so we can just force attribute_buf to be large enough and
	 * then transfer data without any checks for enough space.  We need to do
	 * it this way because enlarging attribute_buf mid-stream would invalidate
	 * pointers already stored into cstate->raw_fields[].
	 */
	if (cstate->attribute_buf.maxlen <= cstate->line_buf.len)
		enlargeStringInfo(&cstate->attribute_buf, cstate->line_buf.len);
	output_ptr = cstate->attribute_buf.data;

	/* set pointer variables for loop */
	cur_ptr = cstate->line_buf.data + cstate->line_buf.cursor;
	line_end_ptr = cstate->line_buf.data + cstate->line_buf.len;

	/* Outer loop iterates over fields */
	fieldno = 0;
	for (;;)
	{
		bool		found_delim = false;
		char	   *start_ptr;
		char	   *end_ptr;
		int			input_len;
		bool		saw_non_ascii = false;

		/*
		 * In QD, stop once we have processed the last field we need in the QD.
		 */
		if (fieldno == stop_processing_at_field)
		{
			cstate->stopped_processing_at_delim = true;
			break;
		}

		/* Make sure there is enough space for the next value */
		if (fieldno >= cstate->max_fields)
		{
			cstate->max_fields *= 2;
			cstate->raw_fields =
				repalloc(cstate->raw_fields, cstate->max_fields * sizeof(char *));
		}

		/* Remember start of field on both input and output sides */
		start_ptr = cur_ptr;
		cstate->raw_fields[fieldno] = output_ptr;

		/*
		 * Scan data for field.
		 *
		 * Note that in this loop, we are scanning to locate the end of field
		 * and also speculatively performing de-escaping.  Once we find the
		 * end-of-field, we can match the raw field contents against the null
		 * marker string.  Only after that comparison fails do we know that
		 * de-escaping is actually the right thing to do; therefore we *must
		 * not* throw any syntax errors before we've done the null-marker
		 * check.
		 */
		for (;;)
		{
			char		c;

			end_ptr = cur_ptr;
			if (cur_ptr >= line_end_ptr)
				break;
			c = *cur_ptr++;
			if (c == delimc && !delim_off)
			{
				found_delim = true;
				break;
			}
			if (c == escapec)
			{
				if (cur_ptr >= line_end_ptr)
					break;
				c = *cur_ptr++;
				switch (c)
				{
					case '0':
					case '1':
					case '2':
					case '3':
					case '4':
					case '5':
					case '6':
					case '7':
						{
							/* handle \013 */
							int			val;

							val = OCTVALUE(c);
							if (cur_ptr < line_end_ptr)
							{
								c = *cur_ptr;
								if (ISOCTAL(c))
								{
									cur_ptr++;
									val = (val << 3) + OCTVALUE(c);
									if (cur_ptr < line_end_ptr)
									{
										c = *cur_ptr;
										if (ISOCTAL(c))
										{
											cur_ptr++;
											val = (val << 3) + OCTVALUE(c);
										}
									}
								}
							}
							c = val & 0377;
							if (c == '\0' || IS_HIGHBIT_SET(c))
								saw_non_ascii = true;
						}
						break;
					case 'x':
						/* Handle \x3F */
						if (cur_ptr < line_end_ptr)
						{
							char		hexchar = *cur_ptr;

							if (isxdigit((unsigned char) hexchar))
							{
								int			val = GetDecimalFromHex(hexchar);

								cur_ptr++;
								if (cur_ptr < line_end_ptr)
								{
									hexchar = *cur_ptr;
									if (isxdigit((unsigned char) hexchar))
									{
										cur_ptr++;
										val = (val << 4) + GetDecimalFromHex(hexchar);
									}
								}
								c = val & 0xff;
								if (c == '\0' || IS_HIGHBIT_SET(c))
									saw_non_ascii = true;
							}
						}
						break;
					case 'b':
						c = '\b';
						break;
					case 'f':
						c = '\f';
						break;
					case 'n':
						c = '\n';
						break;
					case 'r':
						c = '\r';
						break;
					case 't':
						c = '\t';
						break;
					case 'v':
						c = '\v';
						break;

						/*
						 * in all other cases, take the char after '\'
						 * literally
						 */
				}
			}

			/* Add c to output string */
			*output_ptr++ = c;
		}

		/* Check whether raw input matched null marker */
		input_len = end_ptr - start_ptr;
		if (input_len == cstate->null_print_len &&
			strncmp(start_ptr, cstate->null_print, input_len) == 0)
			cstate->raw_fields[fieldno] = NULL;
		else
		{
			/*
			 * At this point we know the field is supposed to contain data.
			 *
			 * If we de-escaped any non-7-bit-ASCII chars, make sure the
			 * resulting string is valid data for the db encoding.
			 */
			if (saw_non_ascii)
			{
				char	   *fld = cstate->raw_fields[fieldno];

				pg_verifymbstr(fld, output_ptr - fld, false);
			}
		}

		/* Terminate attribute value in output area */
		*output_ptr++ = '\0';

		fieldno++;
		/* Done if we hit EOL instead of a delim */
		if (!found_delim)
		{
			cstate->stopped_processing_at_delim = false;
			break;
		}
	}

	/*
	 * Make note of the stopping point in 'line_buf.cursor', so that we
	 * can send the rest to the QE later.
	 */
	cstate->line_buf.cursor = cur_ptr - cstate->line_buf.data;

	/* Clean up state of attribute_buf */
	output_ptr--;
	Assert(*output_ptr == '\0');
	cstate->attribute_buf.len = (output_ptr - cstate->attribute_buf.data);

	return fieldno;
}

/*
 * Parse the current line into separate attributes (fields),
 * performing de-escaping as needed.  This has exactly the same API as
 * CopyReadAttributesText, except we parse the fields according to
 * "standard" (i.e. common) CSV usage.
 */
static int
CopyReadAttributesCSV(PxfCopyState cstate, int stop_processing_at_field)
{
	char		delimc = cstate->delim[0];
	bool		delim_off = cstate->delim_off;
	char		quotec = cstate->quote[0];
	char		escapec = cstate->escape[0];
	int			fieldno;
	char	   *output_ptr;
	char	   *cur_ptr;
	char	   *line_end_ptr;

	/*
	 * We need a special case for zero-column tables: check that the input
	 * line is empty, and return.
	 */
	if (cstate->max_fields <= 0)
	{
		if (cstate->line_buf.len != 0)
			ereport(ERROR,
					(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
					 errmsg("extra data after last expected column")));
		return 0;
	}

	resetStringInfo(&cstate->attribute_buf);

	/*
	 * The de-escaped attributes will certainly not be longer than the input
	 * data line, so we can just force attribute_buf to be large enough and
	 * then transfer data without any checks for enough space.  We need to do
	 * it this way because enlarging attribute_buf mid-stream would invalidate
	 * pointers already stored into cstate->raw_fields[].
	 */
	if (cstate->attribute_buf.maxlen <= cstate->line_buf.len)
		enlargeStringInfo(&cstate->attribute_buf, cstate->line_buf.len);
	output_ptr = cstate->attribute_buf.data;

	/* set pointer variables for loop */
	cur_ptr = cstate->line_buf.data + cstate->line_buf.cursor;
	line_end_ptr = cstate->line_buf.data + cstate->line_buf.len;

	/* Outer loop iterates over fields */
	fieldno = 0;
	for (;;)
	{
		bool		found_delim = false;
		bool		saw_quote = false;
		char	   *start_ptr;
		char	   *end_ptr;
		int			input_len;

		/*
		 * In QD, stop once we have processed the last field we need in the QD.
		 */
		if (fieldno == stop_processing_at_field)
		{
			cstate->stopped_processing_at_delim = true;
			break;
		}

		/* Make sure there is enough space for the next value */
		if (fieldno >= cstate->max_fields)
		{
			cstate->max_fields *= 2;
			cstate->raw_fields =
				repalloc(cstate->raw_fields, cstate->max_fields * sizeof(char *));
		}

		/* Remember start of field on both input and output sides */
		start_ptr = cur_ptr;
		cstate->raw_fields[fieldno] = output_ptr;

		/*
		 * Scan data for field,
		 *
		 * The loop starts in "not quote" mode and then toggles between that
		 * and "in quote" mode. The loop exits normally if it is in "not
		 * quote" mode and a delimiter or line end is seen.
		 */
		for (;;)
		{
			char		c;

			/* Not in quote */
			for (;;)
			{
				end_ptr = cur_ptr;
				if (cur_ptr >= line_end_ptr)
					goto endfield;
				c = *cur_ptr++;
				/* unquoted field delimiter */
				if (c == delimc && !delim_off)
				{
					found_delim = true;
					goto endfield;
				}
				/* start of quoted field (or part of field) */
				if (c == quotec)
				{
					saw_quote = true;
					break;
				}
				/* Add c to output string */
				*output_ptr++ = c;
			}

			/* In quote */
			for (;;)
			{
				end_ptr = cur_ptr;
				if (cur_ptr >= line_end_ptr)
					ereport(ERROR,
							(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
							 errmsg("unterminated CSV quoted field")));

				c = *cur_ptr++;

				/* escape within a quoted field */
				if (c == escapec)
				{
					/*
					 * peek at the next char if available, and escape it if it
					 * is an escape char or a quote char
					 */
					if (cur_ptr < line_end_ptr)
					{
						char		nextc = *cur_ptr;

						if (nextc == escapec || nextc == quotec)
						{
							*output_ptr++ = nextc;
							cur_ptr++;
							continue;
						}
					}
				}

				/*
				 * end of quoted field. Must do this test after testing for
				 * escape in case quote char and escape char are the same
				 * (which is the common case).
				 */
				if (c == quotec)
					break;

				/* Add c to output string */
				*output_ptr++ = c;
			}
		}
endfield:

		/* Terminate attribute value in output area */
		*output_ptr++ = '\0';

		/* Check whether raw input matched null marker */
		input_len = end_ptr - start_ptr;
		if (!saw_quote && input_len == cstate->null_print_len &&
			strncmp(start_ptr, cstate->null_print, input_len) == 0)
			cstate->raw_fields[fieldno] = NULL;

		fieldno++;
		/* Done if we hit EOL instead of a delim */
		if (!found_delim)
		{
			cstate->stopped_processing_at_delim = false;
			break;
		}
	}

	/*
	 * Make note of the stopping point in 'line_buf.cursor', so that we
	 * can send the rest to the QE later.
	 */
	cstate->line_buf.cursor = cur_ptr - cstate->line_buf.data;

	/* Clean up state of attribute_buf */
	output_ptr--;
	Assert(*output_ptr == '\0');
	cstate->attribute_buf.len = (output_ptr - cstate->attribute_buf.data);

	return fieldno;
}

/*
 * Read a binary attribute
 */
static Datum
CopyReadBinaryAttribute(PxfCopyState cstate,
						int column_no, FmgrInfo *flinfo,
						Oid typioparam, int32 typmod,
						bool *isnull)
{
	int32		fld_size;
	Datum		result;

	if (!CopyGetInt32(cstate, &fld_size))
		ereport(ERROR,
				(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
				 errmsg("unexpected EOF in COPY data")));
	if (fld_size == -1)
	{
		*isnull = true;
		return ReceiveFunctionCall(flinfo, NULL, typioparam, typmod);
	}
	if (fld_size < 0)
		ereport(ERROR,
				(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
				 errmsg("invalid field size")));

	/* reset attribute_buf to empty, and load raw data in it */
	resetStringInfo(&cstate->attribute_buf);

	enlargeStringInfo(&cstate->attribute_buf, fld_size);
	if (CopyGetData(cstate, cstate->attribute_buf.data,
					fld_size) != fld_size)
		ereport(ERROR,
				(errcode(ERRCODE_BAD_COPY_FILE_FORMAT),
				 errmsg("unexpected EOF in COPY data")));

	cstate->attribute_buf.len = fld_size;
	cstate->attribute_buf.data[fld_size] = '\0';

	if (typioparam == 1700)
	{
		/* For numeric/decimal type, we use the string converter */
		result = InputFunctionCall(flinfo, cstate->attribute_buf.data,
								   typioparam, typmod);
	}
	else
	{
		/* Call the column type's binary input converter */
		result = ReceiveFunctionCall(flinfo, &cstate->attribute_buf,
									 typioparam, typmod);

		/* Trouble if it didn't eat the whole buffer */
		if (cstate->attribute_buf.cursor != cstate->attribute_buf.len)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
					 errmsg("incorrect binary data format")));
	}

	*isnull = false;
	return result;
}

/*
 * Send text representation of one attribute, with conversion and escaping
 */
#define PXFDUMPSOFAR() \
	do { \
		if (ptr > start) \
			CopySendData(cstate, start, ptr - start); \
	} while (0)

static void
CopyAttributeOutText(PxfCopyState cstate, char *string)
{
	char	   *ptr;
	char	   *start;
	char		c;
	char		delimc = cstate->delim[0];
	char		escapec = cstate->escape[0];

	if (cstate->need_transcoding)
		ptr = pg_server_to_custom(string,
								  strlen(string),
								  cstate->file_encoding,
								  cstate->enc_conversion_proc);
	else
		ptr = string;


	if (cstate->escape_off)
	{
		CopySendData(cstate, ptr, strlen(ptr));
		return;
	}

	/*
	 * We have to grovel through the string searching for control characters
	 * and instances of the delimiter character.  In most cases, though, these
	 * are infrequent.  To avoid overhead from calling CopySendData once per
	 * character, we dump out all characters between escaped characters in a
	 * single call.  The loop invariant is that the data from "start" to "ptr"
	 * can be sent literally, but hasn't yet been.
	 *
	 * We can skip pg_encoding_mblen() overhead when encoding is safe, because
	 * in valid backend encodings, extra bytes of a multibyte character never
	 * look like ASCII.  This loop is sufficiently performance-critical that
	 * it's worth making two copies of it to get the IS_HIGHBIT_SET() test out
	 * of the normal safe-encoding path.
	 */
	if (cstate->encoding_embeds_ascii)
	{
		start = ptr;
		while ((c = *ptr) != '\0')
		{
			if ((unsigned char) c < (unsigned char) 0x20)
			{
				/*
				 * \r and \n must be escaped, the others are traditional. We
				 * prefer to dump these using the C-like notation, rather than
				 * a backslash and the literal character, because it makes the
				 * dump file a bit more proof against Microsoftish data
				 * mangling.
				 */
				switch (c)
				{
					case '\b':
						c = 'b';
						break;
					case '\f':
						c = 'f';
						break;
					case '\n':
						c = 'n';
						break;
					case '\r':
						c = 'r';
						break;
					case '\t':
						c = 't';
						break;
					case '\v':
						c = 'v';
						break;
					default:
						/* If it's the delimiter, must backslash it */
						if (c == delimc)
							break;
						/* All ASCII control chars are length 1 */
						ptr++;
						continue;	/* fall to end of loop */
				}
				/* if we get here, we need to convert the control char */
				PXFDUMPSOFAR();
				PxfCopySendChar(cstate, escapec);
				PxfCopySendChar(cstate, c);
				start = ++ptr;	/* do not include char in next run */
			}
			else if (c == escapec || c == delimc)
			{
				PXFDUMPSOFAR();
				PxfCopySendChar(cstate, escapec);
				start = ptr++;	/* we include char in next run */
			}
			else if (IS_HIGHBIT_SET(c))
				ptr += pg_encoding_mblen(cstate->file_encoding, ptr);
			else
				ptr++;
		}
	}
	else
	{
		start = ptr;
		while ((c = *ptr) != '\0')
		{
			if ((unsigned char) c < (unsigned char) 0x20)
			{
				/*
				 * \r and \n must be escaped, the others are traditional. We
				 * prefer to dump these using the C-like notation, rather than
				 * a backslash and the literal character, because it makes the
				 * dump file a bit more proof against Microsoftish data
				 * mangling.
				 */
				switch (c)
				{
					case '\b':
						c = 'b';
						break;
					case '\f':
						c = 'f';
						break;
					case '\n':
						c = 'n';
						break;
					case '\r':
						c = 'r';
						break;
					case '\t':
						c = 't';
						break;
					case '\v':
						c = 'v';
						break;
					default:
						/* If it's the delimiter, must backslash it */
						if (c == delimc)
							break;
						/* All ASCII control chars are length 1 */
						ptr++;
						continue;	/* fall to end of loop */
				}
				/* if we get here, we need to convert the control char */
				PXFDUMPSOFAR();
				PxfCopySendChar(cstate, escapec);
				PxfCopySendChar(cstate, c);
				start = ++ptr;	/* do not include char in next run */
			}
			else if (c == escapec || c == delimc)
			{
				PXFDUMPSOFAR();
				PxfCopySendChar(cstate, escapec);
				start = ptr++;	/* we include char in next run */
			}
			else
				ptr++;
		}
	}

	PXFDUMPSOFAR();
}

/*
 * Send text representation of one attribute, with conversion and
 * CSV-style escaping
 */
static void
CopyAttributeOutCSV(PxfCopyState cstate, char *string,
					bool use_quote, bool single_attr)
{
	char	   *ptr;
	char	   *start;
	char		c;
	char		delimc = cstate->delim[0];
	char		quotec;
	char		escapec = cstate->escape[0];

	/*
	 * MPP-8075. We may get called with cstate->quote == NULL.
	 */
	if (cstate->quote == NULL)
	{
		quotec = '"';
	}
	else
	{
		quotec = cstate->quote[0];
	}

	/* force quoting if it matches null_print (before conversion!) */
	if (!use_quote && strcmp(string, cstate->null_print) == 0)
		use_quote = true;

	if (cstate->need_transcoding)
		ptr = pg_server_to_custom(string,
								  strlen(string),
								  cstate->file_encoding,
								  cstate->enc_conversion_proc);
	else
		ptr = string;

	/*
	 * Make a preliminary pass to discover if it needs quoting
	 */
	if (!use_quote)
	{
		/*
		 * Because '\.' can be a data value, quote it if it appears alone on a
		 * line so it is not interpreted as the end-of-data marker.
		 */
		if (single_attr && strcmp(ptr, "\\.") == 0)
			use_quote = true;
		else
		{
			char	   *tptr = ptr;

			while ((c = *tptr) != '\0')
			{
				if (c == delimc || c == quotec || c == '\n' || c == '\r')
				{
					use_quote = true;
					break;
				}
				if (IS_HIGHBIT_SET(c) && cstate->encoding_embeds_ascii)
					tptr += pg_encoding_mblen(cstate->file_encoding, tptr);
				else
					tptr++;
			}
		}
	}

	if (use_quote)
	{
		PxfCopySendChar(cstate, quotec);

		/*
		 * We adopt the same optimization strategy as in CopyAttributeOutText
		 */
		start = ptr;
		while ((c = *ptr) != '\0')
		{
			if (c == quotec || c == escapec)
			{
				PXFDUMPSOFAR();
				PxfCopySendChar(cstate, escapec);
				start = ptr;	/* we include char in next run */
			}
			if (IS_HIGHBIT_SET(c) && cstate->encoding_embeds_ascii)
				ptr += pg_encoding_mblen(cstate->file_encoding, ptr);
			else
				ptr++;
		}
		PXFDUMPSOFAR();

		PxfCopySendChar(cstate, quotec);
	}
	else
	{
		/* If it doesn't need quoting, we can just dump it as-is */
		PxfCopySendString(cstate, ptr);
	}
}

/*
 * CopyGetAttnums - build an integer list of attnums to be copied
 *
 * The input attnamelist is either the user-specified column list,
 * or NIL if there was none (in which case we want all the non-dropped
 * columns).
 *
 * We don't include generated columns in the generated full list and we don't
 * allow them to be specified explicitly.  They don't make sense for COPY
 * FROM, but we could possibly allow them for COPY TO.  But this way it's at
 * least ensured that whatever we copy out can be copied back in.
 *
 * rel can be NULL ... it's only used for error reports.
 */
List *
CopyGetAttnums(TupleDesc tupDesc, Relation rel, List *attnamelist)
{
	List	   *attnums = NIL;

	if (attnamelist == NIL)
	{
		/* Generate default column list */
		int			attr_count = tupDesc->natts;
		int			i;

		for (i = 0; i < attr_count; i++)
		{
			if (TupleDescAttr(tupDesc, i)->attisdropped)
				continue;
			if (TupleDescAttr(tupDesc, i)->attgenerated)
				continue;
			attnums = lappend_int(attnums, i + 1);
		}
	}
	else
	{
		/* Validate the user-supplied list and extract attnums */
		ListCell   *l;

		foreach(l, attnamelist)
		{
			char	   *name = strVal(lfirst(l));
			int			attnum;
			int			i;

			/* Lookup column name */
			attnum = InvalidAttrNumber;
			for (i = 0; i < tupDesc->natts; i++)
			{
				Form_pg_attribute att = TupleDescAttr(tupDesc, i);

				if (att->attisdropped)
					continue;
				if (namestrcmp(&(att->attname), name) == 0)
				{
					if (att->attgenerated)
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
								 errmsg("column \"%s\" is a generated column",
										name),
								 errdetail("Generated columns cannot be used in COPY.")));
					attnum = att->attnum;
					break;
				}
			}
			if (attnum == InvalidAttrNumber)
			{
				if (rel != NULL)
					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_COLUMN),
							 errmsg("column \"%s\" of relation \"%s\" does not exist",
									name, RelationGetRelationName(rel))));
				else
					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_COLUMN),
							 errmsg("column \"%s\" does not exist",
									name)));
			}
			/* Check for duplicates */
			if (list_member_int(attnums, attnum))
				ereport(ERROR,
						(errcode(ERRCODE_DUPLICATE_COLUMN),
						 errmsg("column \"%s\" specified more than once",
								name)));
			attnums = lappend_int(attnums, attnum);
		}
	}

	return attnums;
}

/*
 * setEncodingConversionProc
 *
 * COPY and External tables use a custom path to the encoding conversion
 * API because external tables have their own encoding (which is not
 * necessarily client_encoding). We therefore have to set the correct
 * encoding conversion function pointer ourselves, to be later used in
 * the conversion engine.
 *
 * The code here mimics a part of SetClientEncoding() in mbutils.c
 */
static void
setEncodingConversionProc(PxfCopyState cstate, int encoding, bool iswritable)
{
	Oid		conversion_proc;
	
	/*
	 * COPY FROM and RET: convert from file to server
	 * COPY TO   and WET: convert from server to file
	 */
	if (iswritable)
		conversion_proc = FindDefaultConversionProc(GetDatabaseEncoding(), encoding);
	else		
		conversion_proc = FindDefaultConversionProc(encoding, GetDatabaseEncoding());
	
	if (OidIsValid(conversion_proc))
	{
		/* conversion proc found */
		cstate->enc_conversion_proc = palloc(sizeof(FmgrInfo));
		fmgr_info(conversion_proc, cstate->enc_conversion_proc);
	}
	else
	{
		/* no conversion function (both encodings are probably the same) */
		cstate->enc_conversion_proc = NULL;
	}
}

static List *
parse_joined_option_list(char *str, char *delimiter)
{
	char	   *token;
	char	   *comma;
	const char *whitespace = " \t\n\r";
	List	   *cols = NIL;
	int			encoding = GetDatabaseEncoding();

	token = strtokx2(str, whitespace, delimiter, "\"",
					 0, false, false, encoding);

	while (token)
	{
		if (token[0] == ',')
			break;

		cols = lappend(cols, makeString(pstrdup(token)));

		/* consume the comma if any */
		comma = strtokx2(NULL, whitespace, delimiter, "\"",
						 0, false, false, encoding);
		if (!comma || comma[0] != ',')
			break;

		token = strtokx2(NULL, whitespace, delimiter, "\"",
						 0, false, false, encoding);
	}

	return cols;
}
