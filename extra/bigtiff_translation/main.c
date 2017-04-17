#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include "tiff.h"
#include "checks.h"


/* mapping */
typedef enum { UNKNOWN, A, B, C, D, AD, BD, CD } soil_type;
#define MAPPINGS (0x400000)


void load_mapping(const char * filename, soil_type * mapping)
{
  char soil_type[0x0f];
  FILE * fp = NULL;

  /* prepare to load mapping from CSV file */
  fp = fopen(filename, "r");
  fscanf(fp, "%s\n", soil_type);

  /* load mapping from CSV file */
  for (int values = 0, unwanted = 0, mukey = 0; values != EOF;)
    {
      values = fscanf(fp, "%d,%[ABCD/],%d\n", &unwanted, soil_type, &mukey);

      if (values != 3)
	{
	  fscanf(fp, "%s\n", soil_type);
	  continue;
	}

      if (!strcmp(soil_type, "A"))
	mapping[mukey] = A;
      else if (!strcmp(soil_type, "B"))
	mapping[mukey] = B;
      else if (!strcmp(soil_type, "C"))
	mapping[mukey] = C;
      else if (!strcmp(soil_type, "D"))
	mapping[mukey] = D;
      else if (!strcmp(soil_type, "A/B"))
	mapping[mukey] = AD;
      else if (!strcmp(soil_type, "B/D"))
	mapping[mukey] = BD;
      else if (!strcmp(soil_type, "C/D"))
	mapping[mukey] = CD;
    }

  fclose(fp);
}

void map_file(const char * filename, void ** base_ptr, struct stat * info)
{
  int fd = open(filename, O_RDWR);

  if (fd < 0)
    {
      fprintf(stderr, "Unable to open %s in read/write mode.\n", filename);
      exit(-1);
    }

  fstat(fd, info);
  *base_ptr = mmap(NULL, info->st_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  close(fd);
}

void unmap_file(void * base_ptr, struct stat * info)
{
  msync(base_ptr, info->st_size, MS_SYNC);
  munmap(base_ptr, info->st_size);
}

int main(int argc, char ** argv)
{
  soil_type * mapping;
  void * base_ptr;
  struct stat info;
  bigtiff_header * header;
  bigtiff_tag * tag_table;
  uint64_t ifd_offset;
  uint64_t tag_count;
  uint64_t tile_count;
  uint64_t offsets_offset;
  uint64_t lengths_offset;
  uint64_t * offsets = NULL;
  uint64_t * lengths = NULL;

  /* check arguments */
  if (argc != 3)
    {
      fprintf(stderr, "usage: %s <tiff> <translation table>\n", argv[0]);
      exit(-1);
    }

  /* load the mukey -> soil mapping */
  mapping = calloc(MAPPINGS, sizeof(soil_type));
  load_mapping(argv[2], mapping);

  /* map the source tiff into memory */
  map_file(argv[1], &base_ptr, &info);

  /* check out the header */
  header = base_ptr;
  header_check(base_ptr);
  ifd_check(base_ptr);

  /* check the format of the file */
  ifd_offset = header->first_ifd;
  tag_count = *(uint64_t *)(base_ptr + ifd_offset);
  tag_table = base_ptr + ifd_offset + sizeof(uint64_t);
  format_check(tag_table, tag_count);

  /* get numbers of tiles, their locations and lengths */
  get_tile_info(tag_table, tag_count, &tile_count, &offsets_offset, &lengths_offset);
  fprintf(stdout, "%llu 0x%016llx 0x%016llx\n",
	  (long long int)tile_count,
	  (long long unsigned)offsets_offset,
	  (long long unsigned)lengths_offset);

  offsets = base_ptr + offsets_offset;
  lengths = base_ptr + lengths_offset;

  /* for every tile ... */
  for (int i = 0; i < tile_count; ++i)
    {
      uint32_t * tile = base_ptr + offsets[i];
      unsigned int length = lengths[i] / sizeof(uint32_t);

      if (!(i % (tile_count / 1000)))
	{
	  msync(base_ptr, info.st_size, MS_ASYNC);
	  fprintf(stdout, "%d ", i);
	  fflush(stdout);
	}

      /* ... and for every 32-bit word in the tile ... */
      for (int j = 0; j < length; ++j)
      	{
	  /* ... translate the word.  The 16-slot gap at the beginning
	     of the range is to (hopefully) allow the program to be
	     restarted on a partially-converted raster. */
	  if ((tile[j] > 0x0f) && (tile[j] < MAPPINGS))
	    tile[j] = mapping[tile[j]];
	  else
	    tile[j] = 0;
      	}
    }
  fprintf(stdout, "\n");

  /* clean */
  free(mapping);
  unmap_file(base_ptr, &info);

  /* return */
  return 0;
}
