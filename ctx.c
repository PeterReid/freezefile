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

#include "freezefile.h"
#include "blake2.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

static int do_exec(const char *sql, ctx *c){
  char *sql_errmsg = NULL;
  if( SQLITE_OK==sqlite3_exec(c->db, sql, NULL, NULL, &sql_errmsg) ) return 0;
  
  ctx_errmsg(c, sqlite3_mprintf("An interal error occurred while starting up.\n\n"
                               "Technical details: While executing \"\n"
                               "%s"
                               "\n\", the following error was encountered:\n"
                               "%s", sql, *sql_errmsg));
  sqlite3_free(sql_errmsg);
  return 1;
}

static int do_prepare(const char *sql, ctx *c, sqlite3_stmt **stmt){
  if( SQLITE_OK==sqlite3_prepare_v2(c->db, sql, -1, stmt, NULL) ) return 0;
  ctx_errmsg(c, sqlite3_mprintf("An internal error occurred while starting up.\n\n"
                                "Technical details: While preparing \"\n"
                                "%s"
                                "\n\", the following error was encountered:\n"
                                "%s", sql, sqlite3_errmsg(c->db)));
  return 1;
}

void ctx_errmsg(ctx *c, char *errmsg){
  if( c->errtype==CTX_ERR_NONE ){
    if( errmsg ){
      c->errtype = CTX_ERR_MESSAGE;
      c->errmsg = errmsg;
    }else{
      /* Presumedly, the sqlite3_mprintf failed */
      ctx_errtype(c, CTX_ERR_NO_MEMORY);
    }
  }else{
    sqlite3_free(errmsg);
  }
}
void ctx_errtype(ctx *ctx, int errtype){
  if( ctx->errtype==CTX_ERR_NONE ){
    ctx->errtype = errtype;
  }
}

int ctx_init(ctx *c, const char *path){
  memset(c, 0, sizeof(*c));
  int err = sqlite3_open(path, &c->db);
  if( err ){
    ctx_errmsg(c, sqlite3_mprintf("Can't open database: %s", sqlite3_errmsg(c->db)));
    return 1;
  }
  
  sqlite3_busy_timeout(c->db, 5000);
  
  if( do_exec("CREATE TABLE IF NOT EXISTS chunk"
              "(chunk_id INTEGER PRIMARY KEY AUTOINCREMENT"
              ",hash BLOB"
              ",contents BLOB"
              ")", c)
   || do_exec("CREATE TABLE IF NOT EXISTS file"
              "(file_id INTEGER PRIMARY KEY AUTOINCREMENT"
              ",path TEXT"
              ")", c)
   || do_exec("CREATE TABLE IF NOT EXISTS revision"
              "(revision_id INTEGER PRIMARY KEY AUTOINCREMENT"
              ",file_id INT"
              ",counter INT"
              ",time TEXT"
              ",FOREIGN KEY(file_id) REFERENCES file(file_id)"
              ")", c)
   || do_exec("CREATE TABLE IF NOT EXISTS segment"
              "(segment_id INTEGER PRIMARY KEY AUTOINCREMENT"
              ",revision_id INT NOT NULL"
              ",sequence INT NOT NULL"
              ",chunk_id INT NOT NULL"
              ",FOREIGN KEY(revision_id) REFERENCES revision(revision_id)"
              ",FOREIGN KEY(chunk_id) REFERENCES chunk(chunk_id)"
              ")", c)
   || do_exec("CREATE TABLE IF NOT EXISTS segment"
              "(segment_id INTEGER PRIMARY KEY AUTOINCREMENT"
              ",revision_id INT NOT NULL"
              ",sequence INT NOT NULL"
              ",chunk_id INT NOT NULL"
              ",FOREIGN KEY(revision_id) REFERENCES revision(revision_id)"
              ",FOREIGN KEY(chunk_id) REFERENCES chunk(chunk_id)"
              ")", c)
   || do_exec("CREATE TRIGGER IF NOT EXISTS revision_autocounter"
              "AFTER INSERT ON revision BEGIN "
              "UPDATE revision SET counter = 1+IFNULL((SELECT MAX(counter) FROM revision WHERE revision.file_id = NEW.file_id), 0) WHERE revision_id = NEW.revision_id;"
              "END", c)
  ){
    return 1;
  }

  if( do_prepare("BEGIN TRANSACTION", c, &c->begin_transaction)
   || do_prepare("ROLLBACK", c, &c->rollback)
   || do_prepare("COMMIT", c, &c->commit)
   || do_prepare("SELECT file_id FROM file WHERE path = ?", c, &c->lookup_file_id)
   || do_prepare("INSERT INTO file(path) VALUES (?)", c, &c->insert_file)
   || do_prepare("SELECT chunk_id FROM chunk WHERE hash = ?", c, &c->find_chunk)
   || do_prepare("INSERT INTO chunk(hash, contents) VALUES (?, ?)", c, &c->insert_chunk)
   || do_prepare("INSERT INTO segment(revision_id, sequence, chunk_id) VALUES (?, ?, ?)", c, &c->insert_segment)
   || do_prepare("INSERT INTO revision(file_id, time) VALUES (?, datetime('now'))", c, &c->insert_revision)
   || do_prepare("SELECT contents FROM chunk"
                 " INNER JOIN segment USING (chunk_id)"
                 " WHERE revision_id = ?"
                 " ORDER BY sequence ASC", c, &c->select_revision_chunks)
  ){
    return 1;
  }
  return 0;
}

int ctx_close(ctx *c){
  sqlite3_close(c->db);
  sqlite3_free(c->errmsg);
}

int ctx_collect_err(ctx *c, int errcode){
  if( errcode==SQLITE_OK || errcode==SQLITE_DONE || errcode==SQLITE_ROW ) return 0;
  
  ctx_errmsg(c, sqlite3_mprintf("An internal error occurred.\n\n"
                              "Technical details: While %s, the following error occured: %s",
                              sqlite3_errmsg(c->db)));
  return 1;
}

static sqlite3_int64 ctx_get_file_id(ctx *c, const char *path){
  c->err_context = "creating a file entry";
  sqlite3_int64 id = 0;
  if( ctx_collect_err(c, sqlite3_reset(c->lookup_file_id)) ) goto out;
  if( ctx_collect_err(c, sqlite3_bind_text(c->lookup_file_id, 1, path, -1, SQLITE_STATIC)) ) goto out;
  int step_result;
  if( ctx_collect_err(c, step_result=sqlite3_step(c->lookup_file_id)) ) goto out;
  if( step_result==SQLITE_ROW ){
    id = sqlite3_column_int64(c->lookup_file_id, 0);
  }else{
    if( ctx_collect_err(c, sqlite3_clear_bindings(c->lookup_file_id)) ) return 0;
    
    /* Insert a new file */
    if( ctx_collect_err(c, sqlite3_reset(c->insert_file)) ) goto insert_out;
    if( ctx_collect_err(c, sqlite3_bind_text(c->insert_file, 1, path, -1, SQLITE_STATIC)) ) goto insert_out;
    if( ctx_collect_err(c, sqlite3_step(c->insert_file)) ) goto insert_out;
    id = sqlite3_last_insert_rowid(c->db);
    
    insert_out:
    ctx_collect_err(c, sqlite3_clear_bindings(c->insert_file));
  }

  out:
  ctx_collect_err(c, sqlite3_clear_bindings(c->lookup_file_id));
  return id;
}

#define MAX_CHUNK_SIZE 8000

static int exec_simple(ctx *c, sqlite3_stmt *stmt){
  int err;
  if( !stmt ){
    ctx_errmsg(c, sqlite3_mprintf("Internal error: uninitialized statement"));
    return 1;
  }
  if( SQLITE_OK != (err=sqlite3_reset(stmt))
   || SQLITE_DONE != (err=sqlite3_step(stmt))
  ){
    ctx_errmsg(c, sqlite3_mprintf("Internal error during executing \"\n"
                                  "%s\n"
                                  "\": %s", sqlite3_sql(stmt), sqlite3_errmsg(c->db)));
    return err;
  }
  return 0;
}

static int ctx_begin_transaction(ctx *c){
  return exec_simple(c, c->begin_transaction);
}
static int ctx_rollback(ctx *c){
  return exec_simple(c, c->rollback);
}
static int ctx_commit(ctx *c){
  return exec_simple(c, c->commit);
}

sqlite3_int64 ctx_find_chunk(ctx *c, unsigned char *hash){
  c->err_context = "finding a data chunk";
  sqlite3_int64 id = 0;
  if( ctx_collect_err(c, sqlite3_reset(c->find_chunk)) ) goto out;
  if( ctx_collect_err(c, sqlite3_bind_blob(c->find_chunk, 1, hash, HASH_LENGTH, SQLITE_STATIC)) ) goto out;
  int step_result;
  if( ctx_collect_err(c, step_result=sqlite3_step(c->find_chunk)) ) goto out;
  if( step_result==SQLITE_ROW ){
    id = sqlite3_column_int64(c->find_chunk, 0);
  }
  
  out:
  sqlite3_clear_bindings(c->find_chunk);
  return id;
}

sqlite3_int64 ctx_store_chunk(ctx *c, unsigned char *hash, unsigned char *data, unsigned int data_len){
  c->err_context = "storing a data chunk";
  
  sqlite3_int64 id = 0;
  if( ctx_collect_err(c, sqlite3_reset(c->insert_chunk)) ) goto out;
  if( ctx_collect_err(c, sqlite3_reset(c->insert_chunk)) ) goto out;
  if( ctx_collect_err(c, sqlite3_bind_blob(c->insert_chunk, 1, hash, HASH_LENGTH, SQLITE_STATIC)) ) goto out;
  if( ctx_collect_err(c, sqlite3_bind_blob(c->insert_chunk, 2, data, data_len, SQLITE_STATIC)) ) goto out;
  if( ctx_collect_err(c, sqlite3_step(c->insert_chunk)) ) goto out;
  id = sqlite3_last_insert_rowid(c->db);
  
  out:
  ctx_collect_err(c, sqlite3_clear_bindings(c->insert_chunk));
  return id;
}

sqlite3_int64 ctx_store_segment(ctx *c, sqlite3_int64 revision_id, unsigned int sequence, sqlite3_int64 chunk_id){
  c->err_context = "storing a segment";
  
  sqlite3_int64 id = 0;
  if( ctx_collect_err(c, sqlite3_reset(c->insert_segment)) ) goto out;
  if( ctx_collect_err(c, sqlite3_reset(c->insert_segment)) ) goto out;
  if( ctx_collect_err(c, sqlite3_bind_int64(c->insert_segment, 1, revision_id)) ) goto out;
  if( ctx_collect_err(c, sqlite3_bind_int64(c->insert_segment, 2, (sqlite3_int64)sequence)) ) goto out;
  if( ctx_collect_err(c, sqlite3_bind_int64(c->insert_segment, 3, chunk_id)) ) goto out;
  if( ctx_collect_err(c, sqlite3_step(c->insert_segment)) ) goto out;
  id = sqlite3_last_insert_rowid(c->db);
  
  out:
  ctx_collect_err(c, sqlite3_clear_bindings(c->insert_segment));
  return id;
}

sqlite3_int64 ctx_add_revision(ctx *c, sqlite3_int64 file_id){
  c->err_context = "adding a revision";
  
  sqlite3_int64 id = 0;
  if( ctx_collect_err(c, sqlite3_reset(c->insert_revision)) ) goto out;
  if( ctx_collect_err(c, sqlite3_reset(c->insert_revision)) ) goto out;
  if( ctx_collect_err(c, sqlite3_bind_int64(c->insert_revision, 1, file_id)) ) goto out;
  if( ctx_collect_err(c, sqlite3_step(c->insert_revision)) ) goto out;
  id = sqlite3_last_insert_rowid(c->db);
  
  out:
  ctx_collect_err(c, sqlite3_clear_bindings(c->insert_revision));
  return id;
}

typedef struct handler_ctx {
  ctx *c;
  int64_t revision_id;
} handler_ctx;

static int handle_chunk(unsigned int sequence, unsigned char *data, int data_len, void *ptr){
  handler_ctx *info = (handler_ctx *)ptr;
  ctx *c = info->c;
  unsigned char hash[HASH_LENGTH];
  if( blake2b(hash, data, NULL, HASH_LENGTH, (uint64_t)data_len, 0) ) return 1;
  
  printf("Chunk: len=%d, hash=",data_len);
  int i;
  for( i=0; i<HASH_LENGTH; i++ ){
    printf("%02x", hash[i]);
  }
  printf("\n");
  
  sqlite3_int64 chunk_id = ctx_find_chunk(c, hash);
  if( chunk_id==0 ){
    chunk_id = ctx_store_chunk(c, hash, data, data_len);
    if( chunk_id==0 ) return 1;
  }
  ctx_store_segment(c, info->revision_id, sequence, chunk_id);
  
  return 0;
}

int ctx_ingest(ctx *c, const char *path){
  unsigned char *chunk_buf = (unsigned char *)malloc(MAX_CHUNK_SIZE);
  if( !chunk_buf ){
    ctx_errtype(c, CTX_ERR_NO_MEMORY);
    return 1;
  }
  
  if( ctx_begin_transaction(c) ) return 1;
  int file_id = ctx_get_file_id(c, path);
  int revision_id = ctx_add_revision(c, file_id);
  if( file_id==0 ) goto error_out;
  
  handler_ctx info;
  info.c = c;
  info.revision_id = revision_id;
  if( file_to_chunks(path, chunk_buf, MAX_CHUNK_SIZE, handle_chunk, &info) ){
    ctx_errmsg(c, sqlite3_mprintf("Error reading \"%s\"", path));
  }
  
  ctx_commit(c);
  
  return 0;
error_out:
  ctx_rollback(c);
  return 1;
}

int ctx_spew(ctx *c, const char *dest_path, sqlite3_int64 revision_id){
  /* TODO: Consider encoding of dest_path */
  FILE *f = fopen(dest_path, "wb+");
  if( !f ){
    ctx_errmsg(c, sqlite3_mprintf("Could not write to %s", dest_path));
    return 1;
  }
  
  if( ctx_collect_err(c, sqlite3_reset(c->select_revision_chunks)) ) goto out;
  if( ctx_collect_err(c, sqlite3_bind_int64(c->select_revision_chunks, 1, revision_id)) ) goto out;
  int step_result;
  while( 0==ctx_collect_err(c, step_result=sqlite3_step(c->select_revision_chunks)) && step_result==SQLITE_ROW) {
    const void *contents = sqlite3_column_blob(c->select_revision_chunks, 0);
    int contents_len = sqlite3_column_bytes(c->select_revision_chunks, 0);
    if( contents==NULL || contents_len<=0){
      ctx_errmsg(c, sqlite3_mprintf("Got an invalid file chunk while restoring a revision"));
      goto out;
    }
    size_t written = fwrite(contents, 1, (unsigned int)contents_len, f);
    if( written!=(unsigned int)contents_len ){
      ctx_errmsg(c, sqlite3_mprintf("Error writing to %s", dest_path));
      goto out;
    }
  }
  
  out:
  
  ctx_collect_err(c, sqlite3_clear_bindings(c->select_revision_chunks));
  fclose(f);
  return c->errtype != CTX_ERR_NONE;
}