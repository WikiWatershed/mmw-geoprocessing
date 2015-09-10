#ifndef __TIFF_H__
#define __TIFF_H__

#include <stdint.h>

typedef struct
{
  uint16_t order;
  uint16_t version;
  uint16_t offset_length;
  uint16_t unused;
  uint64_t first_ifd;
} __attribute__((packed)) bigtiff_header;

typedef struct
{
  uint16_t tag_code;
  uint16_t datatype;
  uint64_t number;
  uint64_t data;
} __attribute__((packed)) bigtiff_tag;

#endif
