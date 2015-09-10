#ifndef __CHECKS_H__
#define __CHECKS_H__

#include "tiff.h"

void header_check(const bigtiff_header *);
void ifd_check(const bigtiff_header *);
void format_check(const bigtiff_tag *, uint64_t);
void get_tile_info(const bigtiff_tag *, uint64_t, uint64_t *, uint64_t *, uint64_t *);

#endif
