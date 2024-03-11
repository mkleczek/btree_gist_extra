Extra operators to support more conditions when using btree_gist extension in PostgreSQL.

The main reason why this extension was created is lack of SAOP (Scalar Array OPerator) support in GIST indexes.
In other words GiST indexes do not support filtering using `ANY = (ARRAY[])` filters.
That in turn makes it difficult to use efficiently with partitioning.

Implementing proper SAOP support requires patching core PostgreSQL.
But it is possible (and actually quite easy) to provide additional operators to support required functionality.
This extension initially provides `=|| (text, text[])` operator that can be used in place of `ANY = (text[])`.
