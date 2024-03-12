/* contrib/btree_gist_extra/btree_gist_extra--1.0.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION btree_gist_extra" to load this file. \quit

-- CREATE OR REPLACE FUNCTION gbte_text_in_array(text, text[]) RETURNS boolean IMMUTABLE LANGUAGE sql AS
-- $$SELECT $1 = ANY ($2)$$;

CREATE FUNCTION gbte_text_any_eq_array(text, text[])
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION gbte_text_all_eq_array(text, text[])
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION gbte_text_consistent(internal,anyelement,int2,oid,internal)
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE OPERATOR ||= (
	LEFTARG = text,
	RIGHTARG = text[],
	PROCEDURE = gbte_text_any_eq_array
);

CREATE OPERATOR &&= (
	LEFTARG = text,
	RIGHTARG = text[],
	PROCEDURE = gbte_text_all_eq_array
);

-- Create the operator class
CREATE OPERATOR CLASS gist_extra_text_ops
FOR TYPE text USING gist
AS
	OPERATOR	1	<  ,
	OPERATOR	2	<= ,
	OPERATOR	3	=  ,
	OPERATOR	4	>= ,
	OPERATOR	5	>  ,
	FUNCTION	1	gbte_text_consistent (internal, anyelement, int2, oid, internal),
	FUNCTION	2	gbt_text_union (internal, internal),
	FUNCTION	3	gbt_text_compress (internal),
	FUNCTION	4	gbt_var_decompress (internal),
	FUNCTION	5	gbt_text_penalty (internal, internal, internal),
	FUNCTION	6	gbt_text_picksplit (internal, internal),
	FUNCTION	7	gbt_text_same (gbtreekey_var, gbtreekey_var, internal),
	STORAGE			gbtreekey_var;

ALTER OPERATOR FAMILY gist_extra_text_ops USING gist ADD
	OPERATOR	6	<> (text, text) ,
	OPERATOR	7	||= (text, text[]) ,
	OPERATOR	8	&&= (text, text[]) ,
	FUNCTION	9 (text, text) gbt_var_fetch (internal) ;
