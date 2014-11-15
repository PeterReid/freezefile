#include <stdio.h>
#include "freezefile.h"

int main(int argc, char *args[]){
  ctx c;
  if (ctx_init(&c, "db.freezefile")) goto out;
  
  printf("Ingesting...\n");
  if (ctx_ingest(&c, "test.txt")) goto out;
  
out:
  if( c.errmsg ){
    printf("%s\n", c.errmsg);
  }else if( c.errtype ){
    printf("Error code %d\n", c.errtype);
  }
  ctx_close(&c);
}
