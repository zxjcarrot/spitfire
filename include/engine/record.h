#pragma once

#include <iostream>
#include <string>
#include <cassert>
#include <climits>
#include <thread>

#include "schema.h"
#include "field.h"

namespace storage {

class Record {
 public:

  Record(schema* _sptr, char * data)
      : sptr(_sptr),
        data(data),
        data_len(_sptr->ser_len) {
    assert(data != nullptr);
  }

  // Free non-inlined data
  void ClearData() {
    unsigned int field_itr;
    for (field_itr = 0; field_itr < sptr->num_columns; field_itr++) {
      if (sptr->columns[field_itr].inlined == 0) {
        char* ptr = (char*) get_pointer(field_itr);
        delete ptr;
      }
    }
  }

  void Display() {
    std::string data;

    unsigned int field_itr;
    for (field_itr = 0; field_itr < sptr->num_columns; field_itr++)
      data += get_data(field_itr) + " ";

    printf("record : %p %s \n", this, data.c_str());

  }

  std::string GetData(const int field_id) {
    std::string field;
    FieldInfo finfo = sptr->columns[field_id];
    char type = finfo.type;
    size_t offset = finfo.offset;

    switch (type) {
      case FiledType::INTEGER:
        int ival;
        memcpy(&ival, &(data[offset]), sizeof(int));
        field = std::to_string(ival);
        break;

      case FiledType::DOUBLE:
        double dval;
        memcpy(&dval, &(data[offset]), sizeof(double));
        field = std::to_string(dval);
        break;

      case FiledType::VARCHAR: {
        char* vcval = NULL;
        memcpy(&vcval, &(data[offset]), sizeof(void*));
        if (vcval != NULL) {
          field = std::string(vcval);
        }
      }
        break;

      default:
        std::cout << "Invalid type : " << type << std::endl;
        exit(EXIT_FAILURE);
        break;
    }

    return field;
  }

  void* GetPointer(const int field_id) {
    void* vcval = NULL;
    memcpy(&vcval, &(data[sptr->columns[field_id].offset]), sizeof(void*));
    return vcval;
  }

  void SetData(const int field_id, Record* rec_ptr) {
    char type = sptr->columns[field_id].type;
    size_t offset = sptr->columns[field_id].offset;
    size_t len = sptr->columns[field_id].ser_len;

    switch (type) {
      case FiledType::INTEGER:
      case FiledType::DOUBLE:
      case FiledType::VARCHAR:
        memcpy(&(data[offset]), &(rec_ptr->data[offset]), len);
        break;

      default:
        std::cout << "Invalid type : " << type << std::endl;
        break;
    }
  }

  void SetInt(const int field_id, int ival) {
    //assert(sptr->columns[field_id].type == FiledType::INTEGER);
    memcpy(&(data[sptr->columns[field_id].offset]), &ival, sizeof(int));
  }

  void SetDouble(const int field_id, double dval) {
    //assert(sptr->columns[field_id].type == FiledType::DOUBLE);
    memcpy(&(data[sptr->columns[field_id].offset]), &dval, sizeof(double));
  }

  void SetVarchar(const int field_id, std::string vc_str) {
    //assert(sptr->columns[field_id].type == FiledType::VARCHAR);
    char* vc = (char*) pmalloc((vc_str.size()+1)*sizeof(char));//new char[vc_str.size() + 1];
    strcpy(vc, vc_str.c_str());
    memcpy(&(data[sptr->columns[field_id].offset]), &vc, sizeof(void*));
  }

  void SetPointer(const int field_id, void* pval) {
    //assert(sptr->columns[field_id].type == FiledType::VARCHAR);
    memcpy(&(data[sptr->columns[field_id].offset]), &pval, sizeof(void*));
  }

  schema* sptr;
  char* data;
  size_t data_len;
};

}

