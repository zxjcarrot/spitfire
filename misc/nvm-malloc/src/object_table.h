/* Copyright (c) 2014 Tim Berning */

#ifndef OBJECT_TABLE_H_
#define OBJECT_TABLE_H_

#include "types.h"

#define OT_OK        0
#define OT_FAIL      1
#define OT_DUPLICATE 2

void ot_init(void *nvm_start);

void ot_recover(void *nvm_start);

int ot_insert(const char *id, void *data_ptr);

object_table_entry_t* ot_get(const char *id);

int ot_remove(const char *id);

void ot_teardown();

#endif /* OBJECT_TABLE_H_ */
