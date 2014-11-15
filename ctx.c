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
}

static int do_prepare(const char *sql, ctx *c, sqlite3_stmt **stmt){
  if( SQLITE_OK==sqlite3_prepare_v2(c->db, sql, -1, stmt, NULL) ) return 0;
  
  ctx_errmsg(c, sqlite3_mprintf("An internal error occurred while starting up.\n\n"
                                "Technical details: While preparing \"\n"
                                "%s"
                                "\n\", the following error was encountered:\n"
                                "%s", sql, sqlite3_errmsg(c->db)));
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
  ){
    return 1;
  }
  
  if( do_prepare("BEGIN TRANSACTION", c, &c->begin_transaction)
   || do_prepare("ROLLBACK", c, &c->rollback)
   || do_prepare("COMMIT", c, &c->commit)
  ){
    return 1;
  }
  
  return 0;
}

int ctx_close(ctx *c){
  sqlite3_close(c->db);
  sqlite3_free(c->errmsg);
}

static int ctx_get_file_id(ctx *c, const char *path){
  return 1234;
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

static int handle_chunk(unsigned int sequence, unsigned char *data, int data_len, void *ptr){
  ctx *c = (ctx *)ptr;
  unsigned char hash[32];
  if( blake2b(hash, data, NULL, 32, (uint64_t)data_len, 0) ) return 1;
  
  printf("Chunk: len=%d, hash=",data_len);
  int i;
  for( i=0; i<32; i++ ){
    printf("%02x", hash[i]);
  }
  printf("\n");
  
  return 0;
}

int ctx_ingest(ctx *c, const char *path){
  printf("Beginning\n");
  if( ctx_begin_transaction(c) ) return 1;
  printf("Begat\n");
  int file_id = ctx_get_file_id(c, path);
  if( file_id==0 ) goto error_out;
  
  unsigned char *chunk_buf = (unsigned char *)malloc(MAX_CHUNK_SIZE);
  if( !chunk_buf ){
    ctx_errtype(c, CTX_ERR_NO_MEMORY);
    goto error_out;
  }
  
  printf("Chunking...\n");
  if( file_to_chunks(path, chunk_buf, MAX_CHUNK_SIZE, handle_chunk, c) ){
    ctx_errmsg(c, sqlite3_mprintf("Error reading \"%s\"", path));
  }
  
  return 0;
error_out:
  ctx_rollback(c);
  return 1;
}
