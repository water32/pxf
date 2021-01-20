/*-------------------------------------------------------------------------
 *
 * copy.h
 *	  Definitions for using the POSTGRES copy command.
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/copy.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef _PXF_COPY_H_
#define _PXF_COPY_H_

#include "commands/trigger.h"
#include "cdb/cdbcopy.h"

typedef int (*copy_data_source_cb) (void *outbuf, int minread, int maxread, void *extra);

/*
 *	Represents the end-of-line terminator type of the input
 */
typedef enum PxfEolType
{
	PXF_EOL_UNKNOWN,
	PXF_EOL_NL,
	PXF_EOL_CR,
	PXF_EOL_CRNL
} PxfEolType;

/*
 * The error handling mode for this data load.
 */
typedef enum PxfCopyErrMode
{
	PXF_ALL_OR_NOTHING,	/* Either all rows or no rows get loaded (the default) */
	PXF_SREH_IGNORE,	/* Sreh - ignore errors (REJECT, but don't log errors) */
	PXF_SREH_LOG		/* Sreh - log errors */
} PxfCopyErrMode;

/*
 * This struct contains all the state variables used throughout a COPY
 * operation. For simplicity, we use the same struct for all variants of COPY,
 * even though some fields are used in only some cases.
 *
 * Multi-byte encodings: all supported client-side encodings encode multi-byte
 * characters by having the first byte's high bit set. Subsequent bytes of the
 * character can have the high bit not set. When scanning data in such an
 * encoding to look for a match to a single-byte (ie ASCII) character, we must
 * use the full pg_encoding_mblen() machinery to skip over multibyte
 * characters, else we might find a false match to a trailing byte. In
 * supported server encodings, there is no possibility of a false match, and
 * it's faster to make useless comparisons to trailing bytes than it is to
 * invoke pg_encoding_mblen() to skip over them. encoding_embeds_ascii is true
 * when we have to do it the hard way.
 */
typedef struct PxfCopyStateData
{
	/* low-level state data */
//	CopyDest	copy_dest;		/* type of copy source/destination */
//	FILE	   *copy_file;		/* used if copy_dest == COPY_FILE */
	StringInfo	fe_msgbuf;		/* used for all dests during COPY TO, only for
								 * dest == COPY_NEW_FE in COPY FROM */
	bool		is_copy_from;	/* COPY TO, or COPY FROM? */
	bool		reached_eof;	/* true if we read to end of copy data (not
								 * all copy_dest types maintain this) */
	PxfEolType	eol_type;		/* EOL type of input */
	char	   *eol_str;		/* optional NEWLINE from command. before eol_type is defined */
	int			file_encoding;	/* file or remote side's character encoding */
	bool		need_transcoding;	/* file encoding diff from server? */
	bool		encoding_embeds_ascii;	/* ASCII can be non-first byte? */

	/* parameters from the COPY command */
	Relation	rel;			/* relation to copy to or from */
	QueryDesc  *queryDesc;		/* executable query to copy from */
	List	   *attnumlist;		/* integer list of attnums to copy */
	List	   *attnamelist;	/* list of attributes by name */
//	char	   *filename;		/* filename, or NULL for STDIN/STDOUT */
//	bool		is_program;		/* is 'filename' a program to popen? */
	copy_data_source_cb data_source_cb; /* function for reading data */
	void	   *data_source_cb_extra;
	bool		binary;			/* binary format? */
	bool		freeze;			/* freeze rows on loading? */
	bool		csv_mode;		/* Comma Separated Value format? */
	bool		header_line;	/* CSV header line? */
	char	   *null_print;		/* NULL marker string (server encoding!) */
	int			null_print_len; /* length of same */
	char	   *null_print_client;	/* same converted to file encoding */
	char	   *delim;			/* column delimiter (must be 1 byte) */
	char	   *quote;			/* CSV quote char (must be 1 byte) */
	char	   *escape;			/* CSV escape char (must be 1 byte) */
	List	   *force_quote;	/* list of column names */
	bool		force_quote_all;	/* FORCE_QUOTE *? */
	bool	   *force_quote_flags;	/* per-column CSV FQ flags */
	List	   *force_notnull;	/* list of column names */
	bool	   *force_notnull_flags;	/* per-column CSV FNN flags */
	List	   *force_null;		/* list of column names */
	bool	   *force_null_flags;	/* per-column CSV FN flags */
	bool		convert_selectively;	/* do selective binary conversion? */
	List	   *convert_select; /* list of column names (can be NIL) */
	bool	   *convert_select_flags;	/* per-column CSV/TEXT CS flags */
	Node	   *whereClause;	/* WHERE condition (or NULL) */
	bool		fill_missing;	/* missing attrs at end of line are NULL */

	SingleRowErrorDesc *sreh;

	/* these are just for error messages, see CopyFromErrorCallback */
	const char *cur_relname;	/* table name for error messages */
	uint64		cur_lineno;		/* line number for error messages */
	const char *cur_attname;	/* current att for error messages */
	const char *cur_attval;		/* current att value for error messages */

	/*
	 * Working state for COPY TO/FROM
	 */
//	CopyDispatchMode dispatch_mode;
	MemoryContext copycontext;	/* per-copy execution context */

	/*
	 * Working state for COPY TO
	 */
	FmgrInfo   *out_functions;	/* lookup info for output functions */
	MemoryContext rowcontext;	/* per-row evaluation context */

	/*
	 * Working state for COPY FROM
	 */
	AttrNumber	num_defaults;
	FmgrInfo	oid_in_function;
	FmgrInfo   *in_functions;	/* array of input functions for each attrs */
	Oid		   *typioparams;	/* array of element types for in_functions */
	int		   *defmap;			/* array of default att numbers */
	ExprState **defexprs;		/* array of default att expressions */
	bool		volatile_defexprs;	/* is any of defexprs volatile? */
	List	   *range_table;
	ExprState  *qualexpr;

	TransitionCaptureState *transition_capture;

	StringInfo	dispatch_msgbuf; /* used in COPY_DISPATCH mode, to construct message
								  * to send to QE. */

	/* Error handling options */
	PxfCopyErrMode	errMode;
	struct CdbSreh *cdbsreh; /* single row error handler */
	int			lastsegid;

	/*
	 * These variables are used to reduce overhead in textual COPY FROM.
	 *
	 * attribute_buf holds the separated, de-escaped text for each field of
	 * the current line.  The CopyReadAttributes functions return arrays of
	 * pointers into this buffer.  We avoid palloc/pfree overhead by re-using
	 * the buffer on each cycle.
	 */
	StringInfoData attribute_buf;

	/* field raw data pointers found by COPY FROM */

	int			max_fields;
	char	  **raw_fields;

	/*
	 * Similarly, line_buf holds the whole input line being processed. The
	 * input cycle is first to read the whole line into line_buf, convert it
	 * to server encoding there, and then extract the individual attribute
	 * fields into attribute_buf.  line_buf is preserved unmodified so that we
	 * can display it in error messages if appropriate.
	 */
	StringInfoData line_buf;
	bool		line_buf_converted; /* converted to server encoding? */
	bool		line_buf_valid; /* contains the row being processed? */

	/*
	 * Finally, raw_buf holds raw data read from the data source (file or
	 * client connection).  CopyReadLine parses this data sufficiently to
	 * locate line boundaries, then transfers the data to line_buf and
	 * converts it.  Note: we guarantee that there is a \0 at
	 * raw_buf[raw_buf_len].
	 */
#define RAW_BUF_SIZE 65536		/* we palloc RAW_BUF_SIZE+1 bytes */
	char	   *raw_buf;
	int			raw_buf_index;	/* next byte to process */
	int			raw_buf_len;	/* total # of bytes stored */

	/* Greenplum Database specific variables */
	FmgrInfo   *enc_conversion_proc; /* conv proc from exttbl encoding to
										server or the other way around */
	bool		escape_off;		/* treat backslashes as non-special? */
	int			first_qe_processed_field;
	List	   *qd_attnumlist;
	List	   *qe_attnumlist;
	bool		stopped_processing_at_delim;

	bool          skip_ext_partition;  /* skip external partition */

	bool		on_segment; /* QE save data files locally */
	bool		ignore_extra_line; /* Don't count CSV header or binary trailer in
									  "processed" line number for on_segment mode*/
//	ProgramPipes	*program_pipes; /* COPY PROGRAM pipes for data and stderr */


	/* Information on the connections to QEs. */
	CdbCopy    *cdbCopy;

	bool		delim_off;		/* delimiter is set to OFF? */

/* end Greenplum Database specific variables */
} PxfCopyStateData;

typedef struct PxfCopyStateData *PxfCopyState;

/*
 * Some platforms like macOS (since Yosemite) already define 64 bit versions
 * of htonl and nhohl so we need to guard against redefinition.
 */
#ifndef htonll
#define htonll(x) ((1==htonl(1)) ? (x) : ((uint64_t)htonl((x) & 0xFFFFFFFF) << 32) | htonl((x) >> 32))
#endif
#ifndef ntohll
#define ntohll(x) ((1==ntohl(1)) ? (x) : ((uint64_t)ntohl((x) & 0xFFFFFFFF) << 32) | ntohl((x) >> 32))
#endif

//extern void DoCopy(ParseState *state, const CopyStmt *stmt,
//				   int stmt_location, int stmt_len,
//				   uint64 *processed);

extern void PxfProcessCopyOptions(ParseState *pstate, PxfCopyState cstate, bool is_from, List *options);

extern PxfCopyState PxfBeginCopyFrom(ParseState *pstate, Relation rel, bool is_program,
							   copy_data_source_cb data_source_cb, void *data_source_cb_extra,
							   List *attnamelist, List *options);
extern PxfCopyState PxfBeginCopy(ParseState *pstate, bool is_from, Relation rel,
						   Oid queryRelId, List *attnamelist,
						   List *options, TupleDesc tupDesc);
//extern CopyState BeginCopyToOnSegment(QueryDesc *queryDesc);
//extern void EndCopyToOnSegment(CopyState cstate);
extern PxfCopyState PxfBeginCopyToForeignTable(Relation forrel, List *options);
extern void PxfEndCopyFrom(PxfCopyState cstate);
extern bool PxfNextCopyFrom(PxfCopyState cstate, ExprContext *econtext,
						 Datum *values, bool *nulls);
//extern bool PxfNextCopyFromRawFields(PxfCopyState cstate,
//								  char ***fields, int *nfields);
//extern void CopyFromErrorCallback(void *arg);

//extern uint64 CopyFrom(CopyState cstate);

//extern DestReceiver *CreateCopyDestReceiver(void);

//extern List *CopyGetAttnums(TupleDesc tupDesc, Relation rel, List *attnamelist);

extern void PxfCopyOneRowTo(PxfCopyState cstate, TupleTableSlot *slot);
//extern void CopyOneCustomRowTo(CopyState cstate, bytea *value);
extern void PxfCopySendEndOfRow(PxfCopyState cstate);
extern char *limit_printout_length(const char *str);
//extern void truncateEol(StringInfo buf, EolType	eol_type);
//extern void truncateEolStr(char *str, EolType eol_type);

#endif							/* _PXF_COPY_H_ */
