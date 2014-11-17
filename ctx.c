/*
    Copyright 2014 Peter Reid

    This file is part of freezefile.

    Freezefile is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    Freezefile is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with Freezefile.  If not, see <http://www.gnu.org/licenses/>.
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
                               "%s", sql, sql_errmsg));
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
  printf("Creating\n");
  if( do_exec("CREATE TABLE IF NOT EXISTS chunk"
              "(chunk_id INTEGER PRIMARY KEY AUTOINCREMENT"
              ",hash BLOB"
              ",body BLOB"
              ")", c)
   || do_exec("CREATE TABLE IF NOT EXISTS snapshot"
              "(snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT"
              ",time TEXT"
              ",note TEXT"
              ")", c)
   || do_exec("CREATE TABLE IF NOT EXISTS content"
              "(content_id INTEGER PRIMARY KEY AUTOINCREMENT"
              ",hash BLOB"
              ")", c)
   || do_exec("CREATE TABLE IF NOT EXISTS file"
              "(file_id INTEGER PRIMARY KEY AUTOINCREMENT"
              ",path TEXT"
              ")", c)
   || do_exec("CREATE TABLE IF NOT EXISTS revision"
              "(revision_id INTEGER PRIMARY KEY AUTOINCREMENT"
              ",file_id INT"
              ",content_id INT"
              ",snapshot_id INT"
              ",FOREIGN KEY(file_id) REFERENCES file(file_id)"
              ",FOREIGN KEY(content_id) REFERENCES file(content_id)"
              ",FOREIGN KEY(snapshot_id) REFERENCES file(snapshot_id)"
              ")", c)
   || do_exec("CREATE TABLE IF NOT EXISTS segment"
              "(segment_id INTEGER PRIMARY KEY AUTOINCREMENT"
              ",content_id INT NOT NULL"
              ",sequence INT NOT NULL"
              ",chunk_id INT NOT NULL"
              ",FOREIGN KEY(content_id) REFERENCES revision(content_id)"
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
  ){
    return 1;
  }

  if( do_prepare("BEGIN TRANSACTION", c, &c->begin_transaction)
   || do_prepare("ROLLBACK", c, &c->rollback)
   || do_prepare("COMMIT", c, &c->commit)
   || do_prepare("INSERT INTO snapshot(time, note) VALUES (datetime('now'), ?)", c, &c->insert_snapshot)
   || do_prepare("SELECT file_id FROM file WHERE path = ?", c, &c->lookup_file_id)
   || do_prepare("INSERT INTO file(path) VALUES (?)", c, &c->insert_file)
   || do_prepare("SELECT chunk_id FROM chunk WHERE hash = ?", c, &c->find_chunk)
   || do_prepare("INSERT INTO chunk(hash, body) VALUES (?, ?)", c, &c->insert_chunk)
   || do_prepare("INSERT INTO segment(content_id, sequence, chunk_id) VALUES (?, ?, ?)", c, &c->insert_segment)
   || do_prepare("INSERT INTO revision(file_id, snapshot_id, content_id) VALUES (?, ?, ?)", c, &c->insert_revision)
   || do_prepare("SELECT body FROM chunk"
                 " INNER JOIN segment USING (chunk_id)"
                 " WHERE content_id = (SELECT content_id FROM revision WHERE revision_id = ?)"
                 " ORDER BY sequence ASC", c, &c->select_revision_chunks)
   || do_prepare("SELECT content_id FROM content WHERE hash = ?", c, &c->select_content_id)
   || do_prepare("INSERT INTO content(hash) VALUES (?)", c, &c->insert_content)
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
  if( ctx_collect_err(c, sqlite3_bind_blob(c->insert_chunk, 1, hash, HASH_LENGTH, SQLITE_STATIC)) ) goto out;
  if( ctx_collect_err(c, sqlite3_bind_blob(c->insert_chunk, 2, data, data_len, SQLITE_STATIC)) ) goto out;
  if( ctx_collect_err(c, sqlite3_step(c->insert_chunk)) ) goto out;
  id = sqlite3_last_insert_rowid(c->db);
  
  out:
  ctx_collect_err(c, sqlite3_clear_bindings(c->insert_chunk));
  return id;
}

sqlite3_int64 ctx_store_segment(ctx *c, sqlite3_int64 content_id, unsigned int sequence, sqlite3_int64 chunk_id){
  c->err_context = "storing a segment";
  
  sqlite3_int64 id = 0;
  if( ctx_collect_err(c, sqlite3_reset(c->insert_segment)) ) goto out;
  if( ctx_collect_err(c, sqlite3_bind_int64(c->insert_segment, 1, content_id)) ) goto out;
  if( ctx_collect_err(c, sqlite3_bind_int64(c->insert_segment, 2, (sqlite3_int64)sequence)) ) goto out;
  if( ctx_collect_err(c, sqlite3_bind_int64(c->insert_segment, 3, chunk_id)) ) goto out;
  if( ctx_collect_err(c, sqlite3_step(c->insert_segment)) ) goto out;
  id = sqlite3_last_insert_rowid(c->db);
  
  out:
  ctx_collect_err(c, sqlite3_clear_bindings(c->insert_segment));
  return id;
}

sqlite3_int64 ctx_get_content_id(ctx *c, unsigned char *hash){
  c->err_context = "finding stored file contents";
  sqlite3_int64 id = 0;
  if( ctx_collect_err(c, sqlite3_reset(c->select_content_id)) ) goto out;
  if( ctx_collect_err(c, sqlite3_bind_blob(c->select_content_id, 1, hash, HASH_LENGTH, SQLITE_STATIC)) ) goto out;
  int step_result;
  if( ctx_collect_err(c, step_result=sqlite3_step(c->select_content_id)) ) goto out;
  if( step_result==SQLITE_ROW ){
    id = sqlite3_column_int64(c->select_content_id, 0);
  }
  
  out:
  sqlite3_clear_bindings(c->select_content_id);
  return id;
}

sqlite3_int64 ctx_insert_content(ctx *c, unsigned char *hash){
  c->err_context = "storing a data chunk";
  
  sqlite3_int64 id = 0;
  if( ctx_collect_err(c, sqlite3_reset(c->insert_content)) ) goto out;
  if( ctx_collect_err(c, sqlite3_bind_blob(c->insert_content, 1, hash, HASH_LENGTH, SQLITE_STATIC)) ) goto out;
  if( ctx_collect_err(c, sqlite3_step(c->insert_content)) ) goto out;
  id = sqlite3_last_insert_rowid(c->db);
  
  out:
  ctx_collect_err(c, sqlite3_clear_bindings(c->insert_content));
  return id;
}
sqlite3_int64 ctx_add_revision(ctx *c, sqlite3_int64 file_id, sqlite3_int64 content_id){
  c->err_context = "adding a revision";
  
  sqlite3_int64 id = 0;
  if( ctx_collect_err(c, sqlite3_reset(c->insert_revision)) ) goto out;
  if( ctx_collect_err(c, sqlite3_reset(c->insert_revision)) ) goto out;
  if( ctx_collect_err(c, sqlite3_bind_int64(c->insert_revision, 1, file_id)) ) goto out;
  if( ctx_collect_err(c, sqlite3_bind_int64(c->insert_revision, 2, c->creating_snapshot_id)) ) goto out;
  if( ctx_collect_err(c, sqlite3_bind_int64(c->insert_revision, 3, content_id)) ) goto out;
  if( ctx_collect_err(c, sqlite3_step(c->insert_revision)) ) goto out;
  id = sqlite3_last_insert_rowid(c->db);
  
  out:
  ctx_collect_err(c, sqlite3_clear_bindings(c->insert_revision));
  return id;
}

typedef struct handler_ctx {
  ctx *c;
  int64_t content_id;
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
  ctx_store_segment(c, info->content_id, sequence, chunk_id);
  
  return 0;
}

sqlite_int64 ctx_ensure_content(ctx *c, FILE *f){
  unsigned char hash[HASH_LENGTH];
  /* This gets used for reading the file once to get the overall hash,
  ** and then again to build chunks if we have a new hash. It is sized
  ** for that second task.  */
  unsigned char buf[MAX_CHUNK_SIZE];
  
  blake2b_state b;
  blake2b_init(&b, HASH_LENGTH);
  int len;
  while( 0 < (len=fread(buf, 1, sizeof(buf), f)) ){
    blake2b_update(&b, buf, len);
  }
  blake2b_final(&b, hash, HASH_LENGTH);
  
  sqlite_int64 content_id = ctx_get_content_id(c, hash);
  printf("Got content id: %lld\n", content_id);
  if( content_id ) return content_id;
  
  content_id = ctx_insert_content(c, hash);
  
  handler_ctx info;
  info.c = c;
  info.content_id = content_id;
  fseek(f, 0, SEEK_SET);
  if( file_to_chunks(f, buf, MAX_CHUNK_SIZE, handle_chunk, &info) ){
    ctx_errmsg(c, sqlite3_mprintf("Error reading file"));
    return 0;
  }
  
  return content_id;
}

int ctx_add_to_snapshot(ctx *c, const char *path, FILE *f){
  printf("Adding to snapshot: %s\n", path);
  int file_id = ctx_get_file_id(c, path);
  if( file_id==0 ) goto error_out;
  
  int content_id = ctx_ensure_content(c, f);
  if( content_id==0 ) goto error_out;
  
  int revision_id = ctx_add_revision(c, file_id, content_id);
  if( file_id==0 ) goto error_out;
  
  return 0;
error_out:
  return 1;
}

int ctx_begin_snapshot(ctx *c, const char *note){
  c->err_context = "beginning a snapshot";
  c->creating_snapshot_id = 0;
  
  if( ctx_begin_transaction(c) ) goto out;
  
  if( ctx_collect_err(c, sqlite3_reset(c->insert_snapshot)) ) goto out;
  if( ctx_collect_err(c, sqlite3_reset(c->insert_snapshot)) ) goto out;
  if( ctx_collect_err(c, sqlite3_bind_text(c->insert_snapshot, 1, note, -1, SQLITE_STATIC)) ) goto out;
  if( ctx_collect_err(c, sqlite3_step(c->insert_snapshot)) ) goto out;
  c->creating_snapshot_id = sqlite3_last_insert_rowid(c->db);
  
  out:
  ctx_collect_err(c, sqlite3_clear_bindings(c->insert_snapshot));
}

int ctx_finish_snapshot(ctx *c){
  return ctx_commit(c);
}

int ctx_abort_snapshot(ctx *c){
  return ctx_rollback(c);
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