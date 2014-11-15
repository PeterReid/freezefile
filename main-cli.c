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

#include <stdio.h>
#include "freezefile.h"

int main(int argc, char *args[]){
  ctx c;
  if (ctx_init(&c, "db.freezefile")) goto out;
  
  if (ctx_ingest(&c, "test.txt")) goto out;
  
out:
  if( c.errmsg ){
    printf("%s\n", c.errmsg);
  }else if( c.errtype ){
    printf("Error code %d\n", c.errtype);
  }
  ctx_close(&c);
}
