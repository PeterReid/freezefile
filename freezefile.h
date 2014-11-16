/*
    Copyright 2014 Peter Reid

    This file is part of filefreeze.

    Filefreeze is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    Foobar is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with Foobar.  If not, see <http://www.gnu.org/licenses/>.
*/

#include "sqlite3.h"

#define CTX_ERR_NONE 0
#define CTX_ERR_MESSAGE 1
#define CTX_ERR_NO_MEMORY 2

typedef struct ctx {
  sqlite3 *db;
  
  sqlite3_stmt *begin_transaction;
  sqlite3_stmt *rollback;
  sqlite3_stmt *commit;
  
  sqlite3_stmt *lookup_file_id;
  sqlite3_stmt *insert_file;
  sqlite3_stmt *find_chunk;
  sqlite3_stmt *insert_chunk;
  sqlite3_stmt *insert_segment;
  sqlite3_stmt *insert_revision;

  sqlite3_stmt *select_revision_chunks;

  int errtype; /* A  CTX_ERR_* constant */
  char *errmsg; /* Allocated with sqlite3_mprintf */
  const char *err_context;
} ctx;

#define HASH_LENGTH 32

int ctx_init(ctx *ctx, const char *path);
int ctx_close(ctx *ctx);
void ctx_errmsg(ctx *ctx, char *errmsg);
void ctx_errtype(ctx *ctx, int errtype);

/* Create a revision and, if necessary, the associated file for the current contents
 * at path.
 */
int ctx_ingest(ctx *c, const char *path);

/*
 * Output a stored file.
 */
int ctx_spew(ctx *c, const char *dest_path, sqlite3_int64 revision_id);

int file_to_chunks(
  const char *path,
  unsigned char *chunk_buf,
  unsigned int chunk_buf_len,
  int (*handle_chunk)(unsigned int sequence, unsigned char *data, int data_len, void *ptr),
  void *ptr
);
