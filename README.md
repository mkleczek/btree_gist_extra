Extra operators to support more conditions when using btree_gist extension in PostgreSQL.

The main reason why this extension was created is lack of SAOP (Scalar Array OPerator) support in GIST indexes.
In other words GiST indexes do not support filtering using `ANY = (ARRAY[])` filters.
That in turn makes it difficult to use efficiently with partitioning.

Implementing proper SAOP support requires patching core PostgreSQL.
But it is possible (and actually quite easy) to provide additional operators to support required functionality.
This extension initially provides the following operators (naming based on `||` and `&&` operators from C-like languages):
### Any equals
`||= (text, text[])` operator that can be used in place of `ANY = (text[])`:

```
SELECT * FROM tbl WHERE col ||= ARRAY['text1', 'text2', 'text3']
```
### All equal
`&&= (text, text[])` operator that can be used in place of `ALL = (text[])`:

```
SELECT * FROM tbl WHERE col &&= ARRAY['text1', 'text2', 'text3']
```
# Usage in GiST indexes
```
CREATE INDEX ind ON tbl USING gist (
  text_column gist_extra_text_ops
);
```
