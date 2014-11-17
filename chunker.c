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
#include <stdint.h>
#include <stdio.h>
#include <string.h>

static uint64_t roll_left(uint64_t x, int amount) {
  return (x<<amount) | (x >> (64 - amount));
}

static uint64_t window_hash(uint8_t *buf, unsigned int buf_begin, uint64_t *byte_hashes){
  uint64_t hash = 0;
  unsigned int i;
  
  for (i=0; i<64; i++) {
    uint64_t byte_hash =  byte_hashes[buf[(i+buf_begin)&63]];
    hash ^= roll_left(byte_hash, i);
  }
  return hash;
}

/* TODO: This algorithm is inefficient in two ways now: lots of unnecessary memmove'ing and
** recomputing the whole rolling hash at once. Also it does lots of little freads, but the
** memmove change should fix that.
*/
int file_to_chunks(
  FILE *f,
  unsigned char *chunk_buf,
  unsigned int chunk_buf_len,
  int (*handle_chunk)(unsigned int sequence, unsigned char *data, int data_len, void *ptr),
  void *ptr
){
  uint64_t byte_hashes[256];
  byte_hashes[0] = 0xfbe8a26b6c81741eULL;
  int i;
  for( i=1; i<256; i++ ){
    byte_hashes[i] = byte_hashes[i-1]*6364136223846793005ULL + 1442695040888963407ULL;
  }
  
  int err = 0;
  char recently_consumed[64];
  unsigned int recently_consumed_idx = 0;
  uint64_t rolling_hash;
  unsigned int chunk_buf_filled = 0;
  unsigned int sequence = 0;
  while( 1 ){ /* segment-generating loop */
    /* Fill the buffer as much as possible */
    unsigned int read_amount = fread(chunk_buf + chunk_buf_filled, 1, chunk_buf_len-chunk_buf_filled, f);
    chunk_buf_filled += read_amount;
    if( chunk_buf_filled==0 ) break;
    
    if( chunk_buf_filled<=64 ){
      /* A short segment! We have no choice about placing the boundary. This ends now. */
      int err = handle_chunk(sequence, chunk_buf, chunk_buf_filled, ptr);
      if( err ) goto out;
      chunk_buf_filled = 0;
    }else{
      unsigned int segment_length;
      /* We scan forward looking for a great segment end. */
      for ( segment_length=0; segment_length<64; segment_length++ ){
        recently_consumed[segment_length] = chunk_buf[segment_length];
      }
      recently_consumed_idx = 0; // we loop back to the beginning
      
      uint64_t threshold = 0xffffFFFFffffFFFFULL / 300;
      while( segment_length<chunk_buf_filled ){
        uint64_t ending_hash = window_hash(recently_consumed, recently_consumed_idx, byte_hashes);
        
        if( ending_hash < threshold ) break;
        
        recently_consumed[recently_consumed_idx] = chunk_buf[segment_length];
        recently_consumed_idx = (recently_consumed_idx+1) & 63;
        
        segment_length++;
      }
      
      int err = handle_chunk(sequence, chunk_buf, segment_length, ptr);
      if( err ) goto out;
      
      memmove(chunk_buf, chunk_buf + segment_length, chunk_buf_filled - segment_length);
      chunk_buf_filled -= segment_length;
    }
    sequence++;
  }
  
out:
  if( f ) fclose(f);
  return err;
}
