#include <stdio.h>
#include <stdlib.h>
#include "tiff.h"

#define LITTLE_ENDIAN (0x4949)
#define MAGIC_NUMBER (0x002b)
#define BITS_PER_SAMPLE (0x0102)
#define COMPRESSION (0x0103)
#define SAMPLES_PER_PIXEL (0x0115)
#define DATA_TYPE (0x0153)
#define PLANAR_FORMAT (0x011c)
#define TILE_OFFSETS (0x0144)
#define TILE_LENGTHS (0x0145)


void header_check(const bigtiff_header * header)
{
  if ((header->version != MAGIC_NUMBER) || (header->unused != 0x0000))
    {
      fprintf(stderr, "Bad format.\n");
      exit(-1);
    }
  else if (header->order != LITTLE_ENDIAN)
    {
      fprintf(stderr, "Little-endian files only.\n");
      exit(-1);
    }
  else if ((header->offset_length != sizeof(uint64_t)) || (sizeof(uint64_t) != 8))
    {
      fprintf(stderr, "Afraid to proceed.\n");
      exit(-1);
    }
}

void ifd_check(const bigtiff_header * header)
{
  uint64_t ifd_offset = 0;
  uint64_t tag_count = 0;
  uint64_t tag_table_offset = 0;
  void * base_ptr = (void *)header;

  ifd_offset = header->first_ifd;
  tag_count = *(uint64_t *)(base_ptr + ifd_offset);
  tag_table_offset = ifd_offset + sizeof(uint64_t);
  if (*(uint64_t *)(base_ptr + tag_table_offset + sizeof(bigtiff_tag) * tag_count))
    {
      fprintf(stderr, "Only know how to handle one IFD.\n");
      exit(-1);
    }
}

void format_check(const bigtiff_tag * table, uint64_t table_size)
{
  for (int i = 0; i < table_size; ++i)
    {
      uint64_t data = table[i].data;

      switch(table[i].tag_code) {
      case BITS_PER_SAMPLE:
        if (data != 32)
          {
            fprintf(stderr, "Expecting 32 bits/sample, found %d.\n", (int)data);
            exit(-1);
          }
        break;
      case COMPRESSION:
        if (data != 1)
          {
            fprintf(stderr, "Expecting no compression.\n");
            exit(-1);
          }
        break;
      case SAMPLES_PER_PIXEL:
        if (data != 1)
          {
            fprintf(stderr, "Expecting 1 sample/pixel, found %d.\n", (int)data);
            exit(-1);
          }
        break;
      case DATA_TYPE:
	if (data != 1)
	  {
	    fprintf(stderr, "Expecting unsigned integer data.\n");
	    exit(-1);
	  }
        break;
      case PLANAR_FORMAT:
	if (data != 1)
	  {
	    fprintf(stderr, "Expecting chunky planar format.\n");
	    exit(-1);
	  }
      default:
        break;
      }
    }
}

void get_tile_info(const bigtiff_tag * table, uint64_t table_size, uint64_t * tile_count, uint64_t * offsets, uint64_t * lengths)
{
  for (int i = 0; i < table_size; ++i)
    {
      if (table[i].tag_code == TILE_OFFSETS)
	{
	  *tile_count = table[i].number;
	  *offsets = table[i].data;
	}
      else if (table[i].tag_code == TILE_LENGTHS)
	*lengths = table[i].data;
    }
}
