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
#include <string.h>
#include <stdlib.h>
#include "freezefile.h"
#include <Windows.h>

/* I couldn't link to wcscpy_s with mingw... */
void wstrcpy_s(WCHAR *dest, size_t capacity, WCHAR *source){
  if( capacity==0 ) return;
  
  size_t i=0;
  while( source[i] ){
    dest[i] = source[i];
    i++;
    if( i==capacity ){
      dest[i-1] = 0;
      return;
    }
  }
  dest[i] = 0;
}

char *wchar_to_utf8(WCHAR *wchars){
  int len = WideCharToMultiByte(CP_UTF8, 0, wchars, -1, NULL, 0, NULL, NULL);
  if (len==0) {
    DWORD err = GetLastError();
    if( !err ) err = ERROR_INVALID_DATA;
    return;
  }
  char *utf8 = malloc(len);
  if( !utf8 ) return NULL;
  
  WideCharToMultiByte(CP_UTF8, 0, wchars, -1, utf8, len, NULL, NULL);
  return utf8;
}

int begin_snapshat_wchar(ctx *c, WCHAR *note){
  char *utf8_note = wchar_to_utf8(note);
  if( !utf8_note ){
    ctx_errtype(c, CTX_ERR_NO_MEMORY);
    return 0;
  }
  if (ctx_begin_snapshot(c, utf8_note) ) return 1;
  free(utf8_note);
  return 0;
}

int add_all(ctx *c, WCHAR *path){
  size_t path_len = wcslen(path);
  WIN32_FIND_DATAW ffd;
  HANDLE hFind = INVALID_HANDLE_VALUE;
  
  wstrcpy_s(path+path_len, MAX_PATH-path_len, L"*");
  
  wprintf(L"Going to add everything in %ls\n", path);
  
  hFind = FindFirstFileW(path, &ffd);
  
  if( INVALID_HANDLE_VALUE==hFind ){
    printf("Not a valid directory\n");
  }
  
  do{
    if( ffd.cFileName[0]=='~' || ffd.cFileName[0]=='.' ){
      continue;
    }
    if (ffd.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY){
      size_t file_len = wcslen(ffd.cFileName);
      printf(TEXT("  %ls   <DIR>\n"), ffd.cFileName);
      wstrcpy_s(path+path_len, MAX_PATH-path_len, ffd.cFileName);
      
      wstrcpy_s(path+path_len+file_len, MAX_PATH-path_len-file_len, L"\\");
      add_all(c, path);
    }else{
      unsigned char hash[HASH_LENGTH];
      wstrcpy_s(path+path_len, MAX_PATH-path_len, ffd.cFileName);
      FILE *f = _wfopen(path, L"rb");
      if( f ){
        wprintf(L"add_to_snapshot(\"%ls\")", path);
        char *utf8_path = wchar_to_utf8(path);
        ctx_add_to_snapshot(c, utf8_path, f);
        free(utf8_path);
        fclose(f);
        /*
        wprintf(L"%ls: ", ffd.cFileName);
        int i;
        for( i=0; i<HASH_LENGTH; i++ ){
          printf("%02x", hash[i]);
        }
        printf("\n");*/
      }else{
        printf("Could not open\n");
      }
    }
  }while (FindNextFileW(hFind, &ffd) != 0);

}

int make_snapshot(ctx *c, WCHAR *path, WCHAR *note){
  printf("Going to begin snapshot\n");
  if( begin_snapshat_wchar(c, note) ) return 1;
  printf("Began snapshot\n");
  
  if( add_all(c, path) ) {
    ctx_abort_snapshot(c);
    return 1;
  }
  
  ctx_finish_snapshot(c);
}

int main(int argc, char *args[]){
  ctx c;
  printf("here");
  if (ctx_init(&c, "db.freezefile")) goto out;
  
  WCHAR path[MAX_PATH] = L".\\proj\\";
  
  make_snapshot(&c, path, L"Initial commit");
  
  //if (ctx_ingest(&c, "test.txt")) goto out;
  
  //if( ctx_spew(&c, "test2.txt", 2) ) goto out;
  
out:
  if( c.errmsg ){
    printf("%s\n", c.errmsg);
  }else if( c.errtype ){
    printf("Error code %d\n", c.errtype);
  }
  ctx_close(&c);
}
