//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tpcc_loader.cpp
//
// Identification: src/main/tpcc/tpcc_loader.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <chrono>
#include <iostream>
#include <ctime>
#include <cstring>
#include <random>

#include "benchmark/tpcc/tpcc_record.h"
#include "benchmark/tpcc/tpcc_loader.h"
#include "benchmark/tpcc/tpcc_configuration.h"
#include "engine/executor.h"

// Logging mode
// extern peloton::LoggingType peloton_logging_mode;

// disable unused const variable warning for clang
#ifdef __APPLE__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-const-variable"
#endif

namespace spitfire {
std::vector<BaseDataTable*> database_tables;
namespace benchmark {
namespace tpcc {

/////////////////////////////////////////////////////////
// Constants
/////////////////////////////////////////////////////////

const size_t name_length = 31;
const size_t middle_name_length = 1;
const size_t data_length = 64;
const size_t state_length = 15;
const size_t zip_length = 8;
const size_t street_length = 31;
const size_t city_length = 31;
const size_t credit_length = 1;
const size_t phone_length = 31;
const size_t dist_length = 31;

double item_min_price = 1.0;
double item_max_price = 100.0;

double warehouse_name_length = 15;
double warehouse_min_tax = 0.0;
double warehouse_max_tax = 0.2;
double warehouse_initial_ytd = 300000.00f;

double district_name_length = 15;
double district_min_tax = 0.0;
double district_max_tax = 0.2;
double district_initial_ytd = 30000.00f;

std::string customers_good_credit = "GC";
std::string customers_bad_credit = "BC";
double customers_bad_credit_ratio = 0.1;
double customers_init_credit_lim = 50000.0;
double customers_min_discount = 0;
double customers_max_discount = 0.5;
double customers_init_balance = -10.0;
double customers_init_ytd = 10.0;
int customers_init_payment_cnt = 1;
int customers_init_delivery_cnt = 0;

double history_init_amount = 10.0;
size_t history_data_length = 32;

int orders_min_ol_cnt = 5;
int orders_max_ol_cnt = 15;
int orders_init_all_local = 1;
int orders_null_carrier_id = 0;
int orders_min_carrier_id = 1;
int orders_max_carrier_id = 10;

int new_orders_per_district = 900;  // 900

int order_line_init_quantity = 5;
int order_line_max_ol_quantity = 10;
double order_line_min_amount = 0.01;
size_t order_line_dist_info_length = 32;

double stock_original_ratio = 0.1;
int stock_min_quantity = 10;
int stock_max_quantity = 100;
int stock_dist_count = 10;

double payment_min_amount = 1.0;
double payment_max_amount = 5000.0;

int stock_min_threshold = 10;
int stock_max_threshold = 20;

double new_order_remote_txns = 0.01;

const int syllable_count = 10;
const char *syllables[syllable_count] = {"BAR", "OUGHT", "ABLE", "PRI",
                                         "PRES", "ESES", "ANTI", "CALLY",
                                         "ATION", "EING"};

const std::string data_constant = std::string("FOO");

NURandConstant nu_rand_const;



/////////////////////////////////////////////////////////
// Globals
/////////////////////////////////////////////////////////

#define THREAD_POOL_SIZE 16
ThreadPool the_tp(THREAD_POOL_SIZE);

/////////////////////////////////////////////////////////
// Create the tables
/////////////////////////////////////////////////////////

WarehouseTable *warehouse_table;
DistrictTable *district_table;
ItemTable *item_table;
CustomerTable *customer_table;
CustomerSKeyTable *customer_skey_table;
HistoryTable *history_table;
StockTable *stock_table;
OrderTable *orders_table;
OrderSKeyTable *orders_skey_table;
NewOrderTable *new_order_table;
OrderLineTable *order_line_table;

static std::atomic<int32_t> history_id(1);

// Records the root pages of all TPC-C tables
struct TPCCDatabaseMetaPage {
    static constexpr pid_t meta_page_pid = 0; // Assume page 0 stores the metadata of TPC-C Database
    struct MetaData {
        lsn_t lsn;
        pid_t warehouse_pk_root_page_pid;
        pid_t warehouse_vtbl_meta_page_pid;
        pid_t district_pk_root_page_pid;
        pid_t district_vtbl_meta_page_pid;
        pid_t item_pk_root_page_pid;
        pid_t item_vtbl_meta_page_pid;
        pid_t customer_pk_root_page_pid;
        pid_t customer_vtbl_meta_page_pid;
        pid_t customer_sk_root_page_pid;
        pid_t customer_sk_vtbl_meta_page_pid;
        pid_t history_pk_root_page_pid;
        pid_t history_vtbl_meta_page_pid;
        int32_t history_id_snapshot;
        pid_t stock_pk_root_page_pid;
        pid_t stock_vtbl_meta_page_pid;
        pid_t orders_pk_root_page_pid;
        pid_t orders_vtbl_meta_page_pid;
        pid_t orders_sk_root_page_pid;
        pid_t orders_sk_vtbl_meta_page_pid;
        pid_t new_order_pk_root_page_pid;
        pid_t new_order_vtbl_meta_page_pid;
        pid_t order_line_pk_root_page_pid;
        pid_t order_line_vtbl_meta_page_pid;

        txn_id_t current_tid_counter;
        rid_t current_row_id_counter;
    };
    union {
        MetaData m;
        Page p;
    };
};


void CreateWarehouseTable(ConcurrentBufferManager *buf_mgr) {
    /*
     CREATE TABLE WAREHOUSE (
     W_ID SMALLINT DEFAULT '0' NOT NULL,
     W_NAME VARCHAR(16) DEFAULT NULL,
     W_STREET_1 VARCHAR(32) DEFAULT NULL,
     W_STREET_2 VARCHAR(32) DEFAULT NULL,
     W_CITY VARCHAR(32) DEFAULT NULL,
     W_STATE VARCHAR(2) DEFAULT NULL,
     W_ZIP VARCHAR(9) DEFAULT NULL,
     W_TAX FLOAT DEFAULT NULL,
     W_YTD FLOAT DEFAULT NULL,
     CONSTRAINT W_PK_ARRAY PRIMARY KEY (W_ID)
     );
     */


    auto primary_index = new ClusteredIndex<int32_t, Warehouse>(buf_mgr);
    pid_t primary_index_root_page_pid = kInvalidPID;
    Status s = primary_index->Init(primary_index_root_page_pid);
    assert(s.ok());
    auto version_table = new PartitionedHeapTable<int32_t, Warehouse>;
    pid_t version_table_meta_page_pid = kInvalidPID;
    s = version_table->Init(version_table_meta_page_pid, buf_mgr, true);
    assert(s.ok());
    warehouse_table = new WarehouseTable(*primary_index, *version_table);
    database_tables.push_back(warehouse_table);
}

void CreateDistrictTable(ConcurrentBufferManager *buf_mgr) {
    /*
     CREATE TABLE DISTRICT (
     D_ID TINYINT DEFAULT '0' NOT NULL,
     D_W_ID SMALLINT DEFAULT '0' NOT NULL REFERENCES WAREHOUSE (W_ID),
     D_NAME VARCHAR(16) DEFAULT NULL,
     D_STREET_1 VARCHAR(32) DEFAULT NULL,
     D_STREET_2 VARCHAR(32) DEFAULT NULL,
     D_CITY VARCHAR(32) DEFAULT NULL,
     D_STATE VARCHAR(2) DEFAULT NULL,
     D_ZIP VARCHAR(9) DEFAULT NULL,
     D_TAX FLOAT DEFAULT NULL,
     D_YTD FLOAT DEFAULT NULL,
     D_NEXT_O_ID INT DEFAULT NULL,
     PRIMARY KEY (D_W_ID,D_ID)
     );
     */

    auto primary_index = new ClusteredIndex<District::DistrictKey, District>(buf_mgr);
    pid_t primary_index_root_page_pid = kInvalidPID;
    Status s = primary_index->Init(primary_index_root_page_pid);
    assert(s.ok());
    auto version_table = new PartitionedHeapTable<District::DistrictKey, District>;
    pid_t version_table_meta_page_pid = kInvalidPID;
    s = version_table->Init(version_table_meta_page_pid, buf_mgr, true);
    assert(s.ok());
    district_table = new DistrictTable(*primary_index, *version_table);
    database_tables.push_back(district_table);
}

void CreateItemTable(ConcurrentBufferManager *buf_mgr) {
    /*
     CREATE TABLE ITEM (
     I_ID INTEGER DEFAULT '0' NOT NULL,
     I_IM_ID INTEGER DEFAULT NULL,
     I_NAME VARCHAR(32) DEFAULT NULL,
     I_PRICE FLOAT DEFAULT NULL,
     I_DATA VARCHAR(64) DEFAULT NULL,
     CONSTRAINT I_PK_ARRAY PRIMARY KEY (I_ID)
     );
     */
    auto primary_index = new ClusteredIndex<int32_t, Item>(buf_mgr);
    pid_t primary_index_root_page_pid = kInvalidPID;
    Status s = primary_index->Init(primary_index_root_page_pid);
    assert(s.ok());
    auto version_table = new PartitionedHeapTable<int32_t, Item>;
    pid_t version_table_meta_page_pid = kInvalidPID;
    s = version_table->Init(version_table_meta_page_pid, buf_mgr, true);
    assert(s.ok());
    item_table = new ItemTable(*primary_index, *version_table);
    database_tables.push_back(item_table);
}

void CreateCustomerTable(ConcurrentBufferManager *buf_mgr) {
    /*
       CREATE TABLE CUSTOMER (
       C_ID INTEGER DEFAULT '0' NOT NULL,
       C_D_ID TINYINT DEFAULT '0' NOT NULL,
       C_W_ID SMALLINT DEFAULT '0' NOT NULL,
       C_FIRST VARCHAR(32) DEFAULT NULL,
       C_MIDDLE VARCHAR(2) DEFAULT NULL,
       C_LAST VARCHAR(32) DEFAULT NULL,
       C_STREET_1 VARCHAR(32) DEFAULT NULL,
       C_STREET_2 VARCHAR(32) DEFAULT NULL,
       C_CITY VARCHAR(32) DEFAULT NULL,
       C_STATE VARCHAR(2) DEFAULT NULL,
       C_ZIP VARCHAR(9) DEFAULT NULL,
       C_PHONE VARCHAR(32) DEFAULT NULL,
       C_SINCE TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
       C_CREDIT VARCHAR(2) DEFAULT NULL,
       C_CREDIT_LIM FLOAT DEFAULT NULL,
       C_DISCOUNT FLOAT DEFAULT NULL,
       C_BALANCE FLOAT DEFAULT NULL,
       C_YTD_PAYMENT FLOAT DEFAULT NULL,
       C_PAYMENT_CNT INTEGER DEFAULT NULL,
       C_DELIVERY_CNT INTEGER DEFAULT NULL,
       C_DATA VARCHAR(500),
       PRIMARY KEY (C_W_ID,C_D_ID,C_ID),
       UNIQUE (C_W_ID,C_D_ID,C_LAST,C_FIRST),
       CONSTRAINT C_FKEY_D FOREIGN KEY (C_D_ID, C_W_ID) REFERENCES DISTRICT (D_ID,
       D_W_ID)
       );
       CREATE INDEX IDX_CUSTOMER ON CUSTOMER (C_W_ID,C_D_ID,C_LAST);
     */
    auto primary_index = new ClusteredIndex<Customer::CustomerKey, Customer>(buf_mgr);
    pid_t primary_index_root_page_pid = kInvalidPID;
    Status s = primary_index->Init(primary_index_root_page_pid);
    assert(s.ok());
    auto version_table = new PartitionedHeapTable<Customer::CustomerKey, Customer>;
    pid_t version_table_meta_page_pid = kInvalidPID;
    s = version_table->Init(version_table_meta_page_pid, buf_mgr, true);
    assert(s.ok());
    customer_table = new CustomerTable(*primary_index, *version_table);
    database_tables.push_back(customer_table);

    auto secondary_index = new ClusteredIndex<CustomerIndexedColumns, CustomerIndexedColumns>(buf_mgr);
    pid_t secondary_index_root_page_pid = kInvalidPID;
    s = secondary_index->Init(secondary_index_root_page_pid);
    assert(s.ok());
    auto secondary_version_table = new PartitionedHeapTable<CustomerIndexedColumns, CustomerIndexedColumns>;
    pid_t secondary_version_table_meta_page_pid = kInvalidPID;
    s = secondary_version_table->Init(secondary_version_table_meta_page_pid, buf_mgr, true);
    assert(s.ok());
    customer_skey_table = new CustomerSKeyTable(*secondary_index, *secondary_version_table);
    database_tables.push_back(customer_skey_table);
}

void CreateHistoryTable(ConcurrentBufferManager *buf_mgr) {
    /*
      CREATE TABLE HISTORY (
      H_C_ID INTEGER DEFAULT NULL,
      H_C_D_ID TINYINT DEFAULT NULL,
      H_C_W_ID SMALLINT DEFAULT NULL,
      H_D_ID TINYINT DEFAULT NULL,
      H_W_ID SMALLINT DEFAULT '0' NOT NULL,
      H_DATE TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
      H_AMOUNT FLOAT DEFAULT NULL,
      H_DATA VARCHAR(32) DEFAULT NULL,
      CONSTRAINT H_FKEY_C FOREIGN KEY (H_C_ID, H_C_D_ID, H_C_W_ID) REFERENCES
      CUSTOMER (C_ID, C_D_ID, C_W_ID),
      CONSTRAINT H_FKEY_D FOREIGN KEY (H_D_ID, H_W_ID) REFERENCES DISTRICT (D_ID,
      D_W_ID)
      );
     */

    auto primary_index = new ClusteredIndex<int32_t, History>(buf_mgr);
    pid_t primary_index_root_page_pid = kInvalidPID;
    Status s = primary_index->Init(primary_index_root_page_pid);
    assert(s.ok());
    auto version_table = new PartitionedHeapTable<int32_t, History>;
    pid_t version_table_meta_page_pid = kInvalidPID;
    s = version_table->Init(version_table_meta_page_pid, buf_mgr, true);
    assert(s.ok());
    history_table = new HistoryTable(*primary_index, *version_table);
    database_tables.push_back(history_table);
    history_id.store(1);
}

void CreateStockTable(ConcurrentBufferManager *buf_mgr) {
    /*
     CREATE TABLE STOCK (
     S_I_ID INTEGER DEFAULT '0' NOT NULL REFERENCES ITEM (I_ID),
     S_W_ID SMALLINT DEFAULT '0 ' NOT NULL REFERENCES WAREHOUSE (W_ID),
     S_QUANTITY INTEGER DEFAULT '0' NOT NULL,
     S_DIST_01 VARCHAR(32) DEFAULT NULL,
     S_DIST_02 VARCHAR(32) DEFAULT NULL,
     S_DIST_03 VARCHAR(32) DEFAULT NULL,
     S_DIST_04 VARCHAR(32) DEFAULT NULL,
     S_DIST_05 VARCHAR(32) DEFAULT NULL,
     S_DIST_06 VARCHAR(32) DEFAULT NULL,
     S_DIST_07 VARCHAR(32) DEFAULT NULL,
     S_DIST_08 VARCHAR(32) DEFAULT NULL,
     S_DIST_09 VARCHAR(32) DEFAULT NULL,
     S_DIST_10 VARCHAR(32) DEFAULT NULL,
     S_YTD INTEGER DEFAULT NULL,
     S_ORDER_CNT INTEGER DEFAULT NULL,
     S_REMOTE_CNT INTEGER DEFAULT NULL,
     S_DATA VARCHAR(64) DEFAULT NULL,
     PRIMARY KEY (S_W_ID,S_I_ID)
     );
     */

    auto primary_index = new ClusteredIndex<Stock::StockKey, Stock>(buf_mgr);
    pid_t primary_index_root_page_pid = kInvalidPID;
    Status s = primary_index->Init(primary_index_root_page_pid);
    assert(s.ok());
    auto version_table = new PartitionedHeapTable<Stock::StockKey, Stock>();
    pid_t version_table_meta_page_pid = kInvalidPID;
    s = version_table->Init(version_table_meta_page_pid, buf_mgr, true);
    assert(s.ok());
    stock_table = new StockTable(*primary_index, *version_table);
    database_tables.push_back(stock_table);
}

void CreateOrdersTable(ConcurrentBufferManager *buf_mgr) {
    /*
     CREATE TABLE ORDERS (
     O_ID INTEGER DEFAULT '0' NOT NULL,
     O_C_ID INTEGER DEFAULT NULL,
     O_D_ID TINYINT DEFAULT '0' NOT NULL,
     O_W_ID SMALLINT DEFAULT '0' NOT NULL,
     O_ENTRY_D TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
     O_CARRIER_ID INTEGER DEFAULT NULL,
     O_OL_CNT INTEGER DEFAULT NULL,
     O_ALL_LOCAL INTEGER DEFAULT NULL,
     PRIMARY KEY (O_W_ID,O_D_ID,O_ID),
     UNIQUE (O_W_ID,O_D_ID,O_C_ID,O_ID),
     CONSTRAINT O_FKEY_C FOREIGN KEY (O_C_ID, O_D_ID, O_W_ID) REFERENCES CUSTOMER
     (C_ID, C_D_ID, C_W_ID)
     );
     CREATE INDEX IDX_ORDERS ON ORDERS (O_W_ID,O_D_ID,O_C_ID);
     */

    auto primary_index = new ClusteredIndex<Order::OrderKey, Order>(buf_mgr);
    pid_t primary_index_root_page_pid = kInvalidPID;
    Status s = primary_index->Init(primary_index_root_page_pid);
    assert(s.ok());
    auto version_table = new PartitionedHeapTable<Order::OrderKey, Order>;
    pid_t version_table_meta_page_pid = kInvalidPID;
    s = version_table->Init(version_table_meta_page_pid, buf_mgr, true);
    assert(s.ok());
    orders_table = new OrderTable(*primary_index, *version_table);
    database_tables.push_back(orders_table);

    auto secondary_index = new ClusteredIndex<OrderIndexedColumns, OrderIndexedColumns>(buf_mgr);
    pid_t secondary_index_root_page_pid = kInvalidPID;
    s = secondary_index->Init(secondary_index_root_page_pid);
    assert(s.ok());
    auto secondary_version_table = new PartitionedHeapTable<OrderIndexedColumns, OrderIndexedColumns>;
    pid_t secondary_version_table_meta_page_pid = kInvalidPID;
    s = secondary_version_table->Init(secondary_version_table_meta_page_pid, buf_mgr, true);
    assert(s.ok());
    orders_skey_table = new OrderSKeyTable(*secondary_index, *secondary_version_table);
    database_tables.push_back(orders_skey_table);
}

void CreateNewOrderTable(ConcurrentBufferManager *buf_mgr) {
    /*
     CREATE TABLE NEW_ORDER (
     NO_O_ID INTEGER DEFAULT '0' NOT NULL,
     NO_D_ID TINYINT DEFAULT '0' NOT NULL,
     NO_W_ID SMALLINT DEFAULT '0' NOT NULL,
     CONSTRAINT NO_PK_TREE PRIMARY KEY (NO_D_ID,NO_W_ID,NO_O_ID),
     CONSTRAINT NO_FKEY_O FOREIGN KEY (NO_O_ID, NO_D_ID, NO_W_ID) REFERENCES
     ORDERS (O_ID, O_D_ID, O_W_ID)
     );
     */

    auto primary_index = new ClusteredIndex<NewOrder::NewOrderKey, NewOrder>(buf_mgr);
    pid_t primary_index_root_page_pid = kInvalidPID;
    Status s = primary_index->Init(primary_index_root_page_pid);
    assert(s.ok());
    auto version_table = new PartitionedHeapTable<NewOrder::NewOrderKey, NewOrder>;
    pid_t version_table_meta_page_pid = kInvalidPID;
    s = version_table->Init(version_table_meta_page_pid, buf_mgr, true);
    assert(s.ok());
    new_order_table = new NewOrderTable(*primary_index, *version_table);
    database_tables.push_back(new_order_table);
}

void CreateOrderLineTable(ConcurrentBufferManager *buf_mgr) {
    /*
     CREATE TABLE ORDER_LINE (
     OL_O_ID INTEGER DEFAULT '0' NOT NULL,
     OL_D_ID TINYINT DEFAULT '0' NOT NULL,
     OL_W_ID SMALLINT DEFAULT '0' NOT NULL,
     OL_NUMBER INTEGER DEFAULT '0' NOT NULL,
     OL_I_ID INTEGER DEFAULT NULL,
     OL_SUPPLY_W_ID SMALLINT DEFAULT NULL,
     OL_DELIVERY_D TIMESTAMP DEFAULT NULL,
     OL_QUANTITY INTEGER DEFAULT NULL,
     OL_AMOUNT FLOAT DEFAULT NULL,
     OL_DIST_INFO VARCHAR(32) DEFAULT NULL,
     PRIMARY KEY (OL_W_ID,OL_D_ID,OL_O_ID,OL_NUMBER),
     CONSTRAINT OL_FKEY_O FOREIGN KEY (OL_O_ID, OL_D_ID, OL_W_ID) REFERENCES
     ORDERS (O_ID, O_D_ID, O_W_ID),
     CONSTRAINT OL_FKEY_S FOREIGN KEY (OL_I_ID, OL_SUPPLY_W_ID) REFERENCES STOCK
     (S_I_ID, S_W_ID)
     );
     CREATE INDEX IDX_ORDER_LINE_TREE ON ORDER_LINE (OL_W_ID,OL_D_ID,OL_O_ID);
     */

    auto primary_index = new ClusteredIndex<OrderLine::OrderLineKey, OrderLine>(buf_mgr);
    pid_t primary_index_root_page_pid = kInvalidPID;
    Status s = primary_index->Init(primary_index_root_page_pid);
    assert(s.ok());
    auto version_table = new PartitionedHeapTable<OrderLine::OrderLineKey, OrderLine>;
    pid_t version_table_meta_page_pid = kInvalidPID;
    s = version_table->Init(version_table_meta_page_pid, buf_mgr, true);
    assert(s.ok());
    order_line_table = new OrderLineTable(*primary_index, *version_table);
    database_tables.push_back(order_line_table);
}

void CreateTPCCDatabase(ConcurrentBufferManager *buf_mgr) {
    // Clean up
    warehouse_table = nullptr;
    district_table = nullptr;
    item_table = nullptr;
    history_table = nullptr;
    history_table = nullptr;
    stock_table = nullptr;
    orders_table = nullptr;
    new_order_table = nullptr;
    order_line_table = nullptr;

    pid_t tpcc_database_meta_page_pid = kInvalidPID;
    Status s = buf_mgr->NewPage(tpcc_database_meta_page_pid);
    assert(s.ok());
    assert(tpcc_database_meta_page_pid == TPCCDatabaseMetaPage::meta_page_pid);


    CreateWarehouseTable(buf_mgr);
    CreateDistrictTable(buf_mgr);
    CreateItemTable(buf_mgr);
    CreateCustomerTable(buf_mgr);
    CreateHistoryTable(buf_mgr);
    CreateStockTable(buf_mgr);
    CreateOrdersTable(buf_mgr);
    CreateNewOrderTable(buf_mgr);
    CreateOrderLineTable(buf_mgr);

    // Record the pids of the primary index root and version table meta page
    ConcurrentBufferManager::PageAccessor accessor;
    s = buf_mgr->Get(TPCCDatabaseMetaPage::meta_page_pid, accessor, ConcurrentBufferManager::INTENT_WRITE_FULL);
    assert(s.ok());
    auto slice = accessor.PrepareForWrite(0, kPageSize);
    auto tpcc_database_meta_page_desc = accessor.GetPageDesc();
    auto tpcc_database_meta_page = reinterpret_cast<TPCCDatabaseMetaPage *>(slice.data());
    tpcc_database_meta_page->m.warehouse_pk_root_page_pid = warehouse_table->GetPrimaryIndex().GetRootPid();
    tpcc_database_meta_page->m.warehouse_vtbl_meta_page_pid = warehouse_table->GetVersionTable().GetMetaPagePid();
    tpcc_database_meta_page->m.district_pk_root_page_pid = district_table->GetPrimaryIndex().GetRootPid();
    tpcc_database_meta_page->m.district_vtbl_meta_page_pid = district_table->GetVersionTable().GetMetaPagePid();
    tpcc_database_meta_page->m.item_pk_root_page_pid = item_table->GetPrimaryIndex().GetRootPid();
    tpcc_database_meta_page->m.item_vtbl_meta_page_pid = item_table->GetVersionTable().GetMetaPagePid();
    tpcc_database_meta_page->m.customer_pk_root_page_pid = customer_table->GetPrimaryIndex().GetRootPid();
    tpcc_database_meta_page->m.customer_vtbl_meta_page_pid = customer_table->GetVersionTable().GetMetaPagePid();
    tpcc_database_meta_page->m.customer_sk_root_page_pid = customer_skey_table->GetPrimaryIndex().GetRootPid();
    tpcc_database_meta_page->m.customer_sk_vtbl_meta_page_pid = customer_skey_table->GetVersionTable().GetMetaPagePid();
    tpcc_database_meta_page->m.history_pk_root_page_pid = history_table->GetPrimaryIndex().GetRootPid();
    tpcc_database_meta_page->m.history_vtbl_meta_page_pid = history_table->GetVersionTable().GetMetaPagePid();
    tpcc_database_meta_page->m.stock_pk_root_page_pid = stock_table->GetPrimaryIndex().GetRootPid();
    tpcc_database_meta_page->m.stock_vtbl_meta_page_pid = stock_table->GetVersionTable().GetMetaPagePid();
    tpcc_database_meta_page->m.orders_pk_root_page_pid = orders_table->GetPrimaryIndex().GetRootPid();
    tpcc_database_meta_page->m.orders_vtbl_meta_page_pid = orders_table->GetVersionTable().GetMetaPagePid();
    tpcc_database_meta_page->m.orders_sk_root_page_pid = orders_skey_table->GetPrimaryIndex().GetRootPid();
    tpcc_database_meta_page->m.orders_sk_vtbl_meta_page_pid = orders_skey_table->GetVersionTable().GetMetaPagePid();
    tpcc_database_meta_page->m.new_order_pk_root_page_pid = new_order_table->GetPrimaryIndex().GetRootPid();
    tpcc_database_meta_page->m.new_order_vtbl_meta_page_pid = new_order_table->GetVersionTable().GetMetaPagePid();
    tpcc_database_meta_page->m.order_line_pk_root_page_pid = order_line_table->GetPrimaryIndex().GetRootPid();
    tpcc_database_meta_page->m.order_line_vtbl_meta_page_pid = order_line_table->GetVersionTable().GetMetaPagePid();

    accessor.FinishAccess();
    buf_mgr->Put(tpcc_database_meta_page_desc);
}

/////////////////////////////////////////////////////////
// Load in the tables
/////////////////////////////////////////////////////////

std::random_device rd;
std::mt19937 rng(rd());

// Create random NURand constants, appropriate for loading the database.
NURandConstant::NURandConstant() {
    c_last = GetRandomInteger(0, 255);
    c_id = GetRandomInteger(0, 1023);
    order_line_itme_id = GetRandomInteger(0, 8191);
}

// A non-uniform random number, as defined by TPC-C 2.1.6. (page 20).
int GetNURand(int a, int x, int y) {
    assert(x <= y);
    int c = nu_rand_const.c_last;

    if (a == 255) {
        c = nu_rand_const.c_last;
    } else if (a == 1023) {
        c = nu_rand_const.c_id;
    } else if (a == 8191) {
        c = nu_rand_const.order_line_itme_id;
    } else {
        assert(false);
    }

    return (((GetRandomInteger(0, a) | GetRandomInteger(x, y)) + c) %
            (y - x + 1)) +
           x;
}

// A last name as defined by TPC-C 4.3.2.3. Not actually random.
std::string GetLastName(int number) {
    assert(number >= 0 && number <= 999);

    int idx1 = number / 100;
    int idx2 = (number / 10 % 10);
    int idx3 = number % 10;

    char lastname_cstr[name_length];
    std::strcpy(lastname_cstr, syllables[idx1]);
    std::strcat(lastname_cstr, syllables[idx2]);
    std::strcat(lastname_cstr, syllables[idx3]);

    return std::string(lastname_cstr);
}

// A non-uniform random last name, as defined by TPC-C 4.3.2.3.
// The name will be limited to maxCID
std::string GetRandomLastName(int max_cid) {
    int min_cid = 999;
    if (max_cid - 1 < min_cid) {
        min_cid = max_cid - 1;
    }

    return GetLastName(GetNURand(255, 0, min_cid));
}

std::string GetRandomAlphaNumericString(const size_t string_length) {
    const char alphanumeric[] =
            "0123456789"
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            "abcdefghijklmnopqrstuvwxyz";

    std::uniform_int_distribution<> dist(0, sizeof(alphanumeric) - 1);

    char repeated_char = alphanumeric[dist(rng)];
    std::string sample(string_length, repeated_char);
    return sample;
}

bool GetRandomBoolean(double ratio) {
    double sample = (double) rand() / RAND_MAX;
    return (sample < ratio) ? true : false;
}

int GetRandomInteger(const int lower_bound, const int upper_bound) {
    std::uniform_int_distribution<> dist(lower_bound, upper_bound);

    int sample = dist(rng);
    return sample;
}

int GetRandomIntegerExcluding(const int lower_bound, const int upper_bound,
                              const int exclude_sample) {
    int sample;
    if (lower_bound == upper_bound) return lower_bound;

    while (1) {
        sample = GetRandomInteger(lower_bound, upper_bound);
        if (sample != exclude_sample) break;
    }
    return sample;
}

double GetRandomDouble(const double lower_bound, const double upper_bound) {
    std::uniform_real_distribution<> dist(lower_bound, upper_bound);

    double sample = dist(rng);
    return sample;
}

double GetRandomFixedPoint(int decimal_places, double minimum, double maximum) {
    assert(decimal_places > 0);
    assert(minimum < maximum);

    int multiplier = 1;
    for (int i = 0; i < decimal_places; ++i) {
        multiplier *= 10;
    }

    int int_min = (int) (minimum * multiplier + 0.5);
    int int_max = (int) (maximum * multiplier + 0.5);

    return GetRandomDouble(int_min, int_max) / (double) (multiplier);
}

std::string GetStreetName() {
    std::vector<std::string> street_names = {
            "5835 Alderson St", "117  Ettwein St", "1400 Fairstead Ln",
            "1501 Denniston St", "898  Flemington St", "2325 Eldridge St",
            "924  Lilac St", "4299 Minnesota St", "5498 Northumberland St",
            "5534 Phillips Ave"};

    std::uniform_int_distribution<> dist(0, street_names.size() - 1);
    return street_names[dist(rng)];
}

std::string GetZipCode() {
    std::vector<std::string> zip_codes = {"15215", "14155", "80284", "61845",
                                          "23146", "21456", "12345", "21561",
                                          "87752", "91095"};

    std::uniform_int_distribution<> dist(0, zip_codes.size() - 1);
    return zip_codes[dist(rng)];
}

std::string GetCityName() {
    std::vector<std::string> city_names = {
            "Madison", "Pittsburgh", "New York", "Seattle", "San Francisco",
            "Berkeley", "Palo Alto", "Los Angeles", "Boston", "Redwood Shores"};

    std::uniform_int_distribution<> dist(0, city_names.size() - 1);
    return city_names[dist(rng)];
}

std::string GetStateName() {
    std::vector<std::string> state_names = {"WI", "PA", "NY", "WA", "CA", "MA"};

    std::uniform_int_distribution<> dist(0, state_names.size() - 1);
    return state_names[dist(rng)];
}

int GetTimeStamp() {
    auto time_stamp = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
    return time_stamp;
}

Item BuildItemTuple(const int item_id) {
    Item i;
    // I_ID
    i.I_ID = item_id;
    // I_IM_ID
    i.I_IM_ID = item_id * 10;
    // I_NAME
    auto i_name = GetRandomAlphaNumericString(name_length);
    strncpy(i.I_NAME, i_name.c_str(), sizeof(i.I_NAME));
    // I_PRICE
    i.I_PRICE = GetRandomDouble(item_min_price, item_max_price);
    // I_DATA
    auto i_data = GetRandomAlphaNumericString(data_length);
    strncpy(i.I_DATA, i_data.c_str(), sizeof(i.I_DATA));
    return i;
}

Warehouse BuildWarehouseTuple(const int warehouse_id) {
    Warehouse w;

    // W_ID
    w.W_ID = warehouse_id;
    // W_NAME
    auto w_name = GetRandomAlphaNumericString(warehouse_name_length);
    strncpy(w.W_NAME, w_name.c_str(), sizeof(w.W_NAME));
    // W_STREET_1, W_STREET_2
    auto w_street = GetStreetName();
    strncpy(w.W_STREET_1, w_street.c_str(), sizeof(w.W_STREET_1));
    strncpy(w.W_STREET_2, w_street.c_str(), sizeof(w.W_STREET_2));
    // W_CITY
    auto w_city = GetCityName();
    strncpy(w.W_CITY, w_city.c_str(), sizeof(w.W_CITY));
    // W_STATE
    auto w_state = GetStateName();
    strncpy(w.W_STATE, w_state.c_str(), sizeof(w.W_STATE));
    // W_ZIP
    auto w_zip = GetZipCode();
    strncpy(w.W_ZIP, w_zip.c_str(), sizeof(w.W_ZIP));
    // W_TAX
    w.W_TAX = GetRandomDouble(warehouse_min_tax, warehouse_max_tax);
    // W_YTD
    w.W_YTD = warehouse_initial_ytd;
    return w;
}

District BuildDistrictTuple(const int district_id, const int warehouse_id) {
    District d;

    // D_ID
    d.D_ID = district_id;

    // D_W_ID
    d.D_W_ID = warehouse_id;

    // D_NAME
    auto d_name = GetRandomAlphaNumericString(district_name_length);
    strncpy(d.D_NAME, d_name.c_str(), sizeof(d.D_NAME));

    // D_STREET_1, D_STREET_2
    auto d_street = GetStreetName();
    strncpy(d.D_STREET_1, d_street.c_str(), sizeof(d.D_STREET_1));
    strncpy(d.D_STREET_2, d_street.c_str(), sizeof(d.D_STREET_2));

    // D_CITY
    auto d_city = GetCityName();
    strncpy(d.D_CITY, d_city.c_str(), sizeof(d.D_CITY));

    // D_STATE
    auto d_state = GetStateName();
    strncpy(d.D_STATE, d_state.c_str(), sizeof(d.D_STATE));

    // D_ZIP
    auto d_zip = GetZipCode();
    strncpy(d.D_ZIP, d_zip.c_str(), sizeof(d.D_ZIP));

    // D_TAX
    d.D_TAX = GetRandomDouble(district_min_tax, district_max_tax);

    // D_YTD
    d.D_YTD = district_initial_ytd;

    // D_NEXT_O_ID
    d.D_NEXT_O_ID = state.customers_per_district + 1;
    return d;
}

Customer BuildCustomerTuple(const int customer_id, const int district_id, const int warehouse_id) {

    // Customer id begins from 0
    assert(customer_id >= 0 && customer_id < state.customers_per_district);

    Customer c;

    memset(&c, 0, sizeof(c));
    // C_ID
    c.C_ID = customer_id;

    // C_D_ID
    c.C_D_ID = district_id;

    // C_W_ID
    c.C_W_ID = warehouse_id;

    // C_FIRST, C_MIDDLE, C_LAST
    auto c_first = GetRandomAlphaNumericString(name_length);

    std::string c_last;

    // Here our customer id begins from 0
    if (customer_id <= 999) {
        c_last = GetLastName(customer_id);
    } else {
        c_last = GetRandomLastName(state.customers_per_district);
    }

    auto c_middle = GetRandomAlphaNumericString(middle_name_length);

    strncpy(c.C_FIRST, c_first.c_str(), sizeof(c.C_FIRST));
    strncpy(c.C_MIDDLE, c_middle.c_str(), sizeof(c.C_MIDDLE));
    strncpy(c.C_LAST, c_last.c_str(), sizeof(c.C_LAST));

    // C_STREET_1, C_STREET_2
    auto c_street = GetStreetName();
    strncpy(c.C_STREET_1, c_street.c_str(), sizeof(c.C_STREET_1));
    strncpy(c.C_STREET_2, c_street.c_str(), sizeof(c.C_STREET_2));

    // C_CITY
    auto c_city = GetCityName();
    strncpy(c.C_CITY, c_city.c_str(), sizeof(c.C_CITY));

    // C_STATE
    auto c_state = GetStateName();
    strncpy(c.C_STATE, c_state.c_str(), sizeof(c.C_STATE));

    // C_ZIP
    auto c_zip = GetZipCode();
    strncpy(c.C_ZIP, c_zip.c_str(), sizeof(c.C_ZIP));

    // C_PHONE
    auto c_phone = GetRandomAlphaNumericString(phone_length);
    strncpy(c.C_PHONE, c_phone.c_str(), sizeof(c.C_PHONE));

    // C_SINCE_TIMESTAMP
    auto c_since_timestamp = GetTimeStamp();
    c.C_SINCE = GetTimeStamp();

    // C_CREDIT
    auto c_bad_credit = GetRandomBoolean(customers_bad_credit_ratio);
    auto c_credit = c_bad_credit ? customers_bad_credit : customers_good_credit;
    memcpy(c.C_CREDIT, c_credit.data(), sizeof(c.C_CREDIT));

    // C_CREDIT_LIM
    c.C_CREDIT_LIM = customers_init_credit_lim;

    // C_DISCOUNT
    c.C_DISCOUNT = GetRandomDouble(customers_min_discount, customers_max_discount);

    // C_BALANCE
    c.C_BALANCE = customers_init_balance;

    // C_YTD_PAYMENT
    c.C_YTD_PAYMENT = customers_init_ytd;

    // C_PAYMENT_CNT
    c.C_PAYMENT_CNT = customers_init_payment_cnt;

    // C_DELIVERY_CNT
    c.C_DELIVERY_CNT = customers_init_delivery_cnt;

    // C_DATA

    auto c_data = GetRandomAlphaNumericString(data_length);
    memcpy(c.C_DATA, c_data.data(), sizeof(c.C_DATA));

    return c;
}

History BuildHistoryTuple(const int customer_id, const int district_id, const int warehouse_id,
                          const int history_district_id, const int history_warehouse_id) {
    History h;
    h.H_ID = history_id.fetch_add(1);
    // H_C_ID
    h.H_C_ID = customer_id;

    // H_C_D_ID
    h.H_C_D_ID = district_id;

    // H_C_W_ID
    h.H_C_W_ID = warehouse_id;

    // H_D_ID
    h.H_D_ID = history_district_id;

    // H_W_ID
    h.H_W_ID = history_warehouse_id;

    // H_DATE
    auto h_date = GetTimeStamp();
    h.H_DATE = h_date;

    // H_AMOUNT
    h.H_AMOUNT = history_init_amount;

    // H_DATA
    auto h_data = GetRandomAlphaNumericString(history_data_length);
    memcpy(h.H_DATA, h_data.data(), sizeof(h.H_DATA));

    return h;
}

Order BuildOrdersTuple(const int orders_id,
                       const int district_id,
                       const int warehouse_id,
                       const bool new_order,
                       const int o_ol_cnt) {
    Order o;

    // O_ID
    o.O_ID = orders_id;

    // O_C_ID
    auto o_c_id = GetRandomInteger(0, state.customers_per_district);
    o.O_C_ID = o_c_id;

    // O_D_ID
    o.O_D_ID = district_id;

    // O_W_ID
    o.O_W_ID = warehouse_id;

    // O_ENTRY_D
    auto o_entry_d = GetTimeStamp();
    o.O_ENTRY_D = o_entry_d;

    // O_CARRIER_ID
    auto o_carrier_id = orders_null_carrier_id;
    if (new_order == false) {
        o_carrier_id =
                GetRandomInteger(orders_min_carrier_id, orders_max_carrier_id);
    }
    o.O_CARRIER_ID = o_carrier_id;

    // O_OL_CNT
    o.O_OL_CNT = o_ol_cnt;

    // O_ALL_LOCAL
    o.O_ALL_LOCAL = orders_init_all_local;

    return o;
}

NewOrder BuildNewOrderTuple(const int orders_id,
                            const int district_id,
                            const int warehouse_id) {
    NewOrder no;

    // NO_O_ID
    no.NO_O_ID = orders_id;

    // NO_D_ID
    no.NO_D_ID = district_id;

    // NO_W_ID
    no.NO_W_ID = warehouse_id;

    return no;
}

OrderLine BuildOrderLineTuple(const int orders_id, const int district_id, const int warehouse_id,
                              const int order_line_id, const int ol_supply_w_id, const bool new_order) {
    OrderLine ol;

    // OL_O_ID
    ol.OL_O_ID = orders_id;

    // OL_D_ID
    ol.OL_D_ID = district_id;

    // OL_W_ID
    ol.OL_W_ID = warehouse_id;

    // OL_NUMBER
    ol.OL_NUMBER = order_line_id;

    // OL_I_ID
    auto ol_i_id = GetRandomInteger(0, state.item_count);
    ol.OL_I_ID = ol_i_id;

    // OL_SUPPLY_W_ID
    ol.OL_SUPPLY_W_ID = ol_supply_w_id;

    // OL_DELIVERY_D
    int64_t ol_delivery_d = GetTimeStamp();
    if (new_order == true) {
        ol_delivery_d = std::numeric_limits<int64_t>::min();
    }
    ol.OL_DELIVERY_D = ol_delivery_d;

    // OL_QUANTITY
    ol.OL_QUANTITY = order_line_init_quantity;

    // OL_AMOUNT
    double ol_amount = 0;
    if (new_order == true) {
        ol_amount = GetRandomDouble(order_line_min_amount,
                                    order_line_max_ol_quantity * item_max_price);
    }
    ol.OL_AMOUNT = ol_amount;

    // OL_DIST_INFO
    auto ol_dist_info = GetRandomAlphaNumericString(order_line_dist_info_length);
    memcpy(ol.OL_DIST_INFO, ol_dist_info.c_str(), sizeof(ol.OL_DIST_INFO));

    return ol;
}

Stock BuildStockTuple(const int stock_id, const int s_w_id) {
    Stock s;

    // S_I_ID
    s.S_I_ID = stock_id;

    // S_W_ID
    s.S_W_ID = s_w_id;

    // S_QUANTITY
    auto s_quantity = GetRandomInteger(stock_min_quantity, stock_max_quantity);
    s.S_QUANTITY = s_quantity;

    // S_DIST_01 .. S_DIST_10
    auto s_dist = GetRandomAlphaNumericString(name_length);
    strncpy(s.S_DIST_01, s_dist.c_str(), sizeof(s.S_DIST_01));
    strncpy(s.S_DIST_02, s_dist.c_str(), sizeof(s.S_DIST_02));
    strncpy(s.S_DIST_03, s_dist.c_str(), sizeof(s.S_DIST_03));
    strncpy(s.S_DIST_04, s_dist.c_str(), sizeof(s.S_DIST_04));
    strncpy(s.S_DIST_05, s_dist.c_str(), sizeof(s.S_DIST_05));
    strncpy(s.S_DIST_06, s_dist.c_str(), sizeof(s.S_DIST_06));
    strncpy(s.S_DIST_07, s_dist.c_str(), sizeof(s.S_DIST_07));
    strncpy(s.S_DIST_08, s_dist.c_str(), sizeof(s.S_DIST_08));
    strncpy(s.S_DIST_09, s_dist.c_str(), sizeof(s.S_DIST_09));
    strncpy(s.S_DIST_10, s_dist.c_str(), sizeof(s.S_DIST_10));

    // S_YTD
    auto s_ytd = 0;
    s.S_YTD = s_ytd;

    // S_ORDER_CNT
    auto s_order_cnt = 0;
    s.S_ORDER_CNT = s_order_cnt;

    // S_REMOTE_CNT
    auto s_remote_cnt = 0;
    s.S_REMOTE_CNT = s_remote_cnt;

    // S_DATA
    auto s_data = GetRandomAlphaNumericString(data_length);
    memcpy(s.S_DATA, s_data.c_str(), sizeof(s.S_DATA));

    return s;
}

void LoadItems(ConcurrentBufferManager *buf_mgr) {

    auto txn_manager = MVTOTransactionManager::GetInstance(buf_mgr);
    auto txn = txn_manager->BeginTransaction();
    for (auto item_itr = 0; item_itr < state.item_count; item_itr++) {
        auto item_tuple = BuildItemTuple(item_itr);
        InsertExecutor<int32_t, Item> executor(*item_table, item_tuple, txn, buf_mgr);
        auto res = executor.Execute();
        assert(res == true);
    }

    auto res = txn_manager->CommitTransaction(txn);
    assert(res == ResultType::SUCCESS);
}

void LoadWarehouses(const int &warehouse_from, const int &warehouse_to, ConcurrentBufferManager *buf_mgr) {
    auto txn_manager = MVTOTransactionManager::GetInstance(buf_mgr);

    // WAREHOUSES
    for (auto warehouse_itr = warehouse_from; warehouse_itr < warehouse_to; warehouse_itr++) {


        auto txn = txn_manager->BeginTransaction();

        auto warehouse_tuple = BuildWarehouseTuple(warehouse_itr);
        InsertExecutor<int32_t, Warehouse> executor(*warehouse_table, warehouse_tuple, txn, buf_mgr);
        auto res = executor.Execute();
        assert(res == true);

        auto txn_res = txn_manager->CommitTransaction(txn);
        assert(txn_res == ResultType::SUCCESS);

        // DISTRICTS
        for (auto district_itr = 0; district_itr < state.districts_per_warehouse;
             district_itr++) {
            auto txn = txn_manager->BeginTransaction();

            auto district_tuple =
                    BuildDistrictTuple(district_itr, warehouse_itr);
            InsertExecutor<District::DistrictKey, District> executor(*district_table, district_tuple, txn, buf_mgr);
            auto res = executor.Execute();
            assert(res == true);

            auto txn_res = txn_manager->CommitTransaction(txn);
            assert(txn_res == ResultType::SUCCESS);

            // CUSTOMERS
            for (auto customer_itr = 0; customer_itr < state.customers_per_district;
                 customer_itr++) {
                auto txn = txn_manager->BeginTransaction();

                auto customer_tuple =
                        BuildCustomerTuple(customer_itr, district_itr, warehouse_itr);

                {
                    InsertExecutor<Customer::CustomerKey, Customer> executor(*customer_table, customer_tuple, txn,
                                                                             buf_mgr);
                    auto res = executor.Execute();
                    assert(res == true);
                }


                // Maintain secondary index as well
                {
                    CustomerIndexedColumns cic;
                    cic.C_ID = customer_tuple.C_ID;
                    cic.C_W_ID = customer_tuple.C_W_ID;
                    cic.C_D_ID = customer_tuple.C_D_ID;
                    memcpy(cic.C_LAST, customer_tuple.C_LAST, sizeof(cic.C_LAST));

                    InsertExecutor<CustomerIndexedColumns, CustomerIndexedColumns> executor(*customer_skey_table, cic,
                                                                                            txn,
                                                                                            buf_mgr);
                    auto res = executor.Execute();
                    assert(res == true);
                }

                // HISTORY

                int history_district_id = district_itr;
                int history_warehouse_id = warehouse_itr;
                auto history_tuple =
                        BuildHistoryTuple(customer_itr, district_itr, warehouse_itr,
                                          history_district_id, history_warehouse_id);
                {
                    InsertExecutor<int32_t, History> executor(*history_table, history_tuple, txn, buf_mgr);
                    auto res = executor.Execute();
                    assert(res == true);
                }

                auto txn_res = txn_manager->CommitTransaction(txn);
                assert(txn_res == ResultType::SUCCESS);

            }  // END CUSTOMERS

            // ORDERS
            for (auto orders_itr = 0; orders_itr < state.customers_per_district;
                 orders_itr++) {
                auto txn = txn_manager->BeginTransaction();

                // New order ?
                auto new_order_threshold =
                        state.customers_per_district - new_orders_per_district;
                bool new_order = (orders_itr > new_order_threshold);
                auto o_ol_cnt = GetRandomInteger(orders_min_ol_cnt, orders_max_ol_cnt);

                auto orders_tuple = BuildOrdersTuple(
                        orders_itr, district_itr, warehouse_itr, new_order, o_ol_cnt);
                {
                    InsertExecutor<Order::OrderKey, Order> executor(*orders_table, orders_tuple, txn, buf_mgr);
                    auto res = executor.Execute();
                    assert(res == true);
                }

                // Maintain secondary index as well
                {
                    OrderIndexedColumns oic;
                    oic.O_W_ID = orders_tuple.O_W_ID;
                    oic.O_D_ID = orders_tuple.O_D_ID;
                    oic.O_C_ID = orders_tuple.O_C_ID;
                    oic.O_ID = orders_tuple.O_ID;
                    InsertExecutor<OrderIndexedColumns, OrderIndexedColumns> executor(*orders_skey_table, oic, txn,
                                                                                      buf_mgr);
                    auto res = executor.Execute();
                    assert(res == true);
                }

                // NEW_ORDER
                if (new_order) {
                    auto new_order_tuple =
                            BuildNewOrderTuple(orders_itr, district_itr, warehouse_itr);
                    InsertExecutor<NewOrder::NewOrderKey, NewOrder> executor(*new_order_table, new_order_tuple, txn,
                                                                             buf_mgr);
                    auto res = executor.Execute();
                    assert(res == true);
                }

                // ORDER_LINE
                for (auto order_line_itr = 0; order_line_itr < o_ol_cnt;
                     order_line_itr++) {
                    int ol_supply_w_id = warehouse_itr;
                    auto order_line_tuple = BuildOrderLineTuple(
                            orders_itr, district_itr, warehouse_itr, order_line_itr,
                            ol_supply_w_id, new_order);
                    {
                        InsertExecutor<OrderLine::OrderLineKey, OrderLine> executor(*order_line_table, order_line_tuple,
                                                                                    txn, buf_mgr);
                        auto res = executor.Execute();
                        assert(res == true);
                    }

                }

                auto txn_res = txn_manager->CommitTransaction(txn);
                assert(txn_res == ResultType::SUCCESS);
            }

        }  // END DISTRICTS

        // STOCK
        for (auto stock_itr = 0; stock_itr < state.item_count; stock_itr++) {
            auto txn = txn_manager->BeginTransaction();

            int s_w_id = warehouse_itr;
            auto stock_tuple = BuildStockTuple(stock_itr, s_w_id);
            InsertExecutor<Stock::StockKey, Stock> executor(*stock_table, stock_tuple, txn, buf_mgr);
            auto res = executor.Execute();
            assert(res == true);

            txn_manager->CommitTransaction(txn);
        }

    }  // END WAREHOUSES
}

void LoadTPCCDatabase(ConcurrentBufferManager *buf_mgr) {

    std::chrono::steady_clock::time_point start_time;
    start_time = std::chrono::steady_clock::now();

    LoadItems(buf_mgr);

    if (state.warehouse_count < state.loader_count) {
        CountDownLatch latch(state.warehouse_count);
        for (int thread_id = 0; thread_id < state.warehouse_count; ++thread_id) {
            int warehouse_from = thread_id;
            int warehouse_to = thread_id + 1;
            the_tp.enqueue([warehouse_from, warehouse_to, buf_mgr, &latch]() {
                LoadWarehouses(warehouse_from, warehouse_to, buf_mgr);
                latch.CountDown();
            });
        }
        latch.Await();
    } else {
        CountDownLatch latch(state.loader_count);
        int warehouse_per_thread = state.warehouse_count / state.loader_count;
        for (int thread_id = 0; thread_id < state.loader_count - 1; ++thread_id) {
            int warehouse_from = warehouse_per_thread * thread_id;
            int warehouse_to = warehouse_per_thread * (thread_id + 1);
            the_tp.enqueue([warehouse_from, warehouse_to, buf_mgr, &latch]() {
                LoadWarehouses(warehouse_from, warehouse_to, buf_mgr);
                latch.CountDown();
            });
        }
        int thread_id = state.loader_count - 1;
        int warehouse_from = warehouse_per_thread * thread_id;
        int warehouse_to = state.warehouse_count;
        the_tp.enqueue([warehouse_from, warehouse_to, buf_mgr, &latch]() {
            LoadWarehouses(warehouse_from, warehouse_to, buf_mgr);
            latch.CountDown();
        });
        latch.Await();
    }

    std::chrono::steady_clock::time_point end_time = std::chrono::steady_clock::now();
    double diff = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
    LOG_INFO("database loading time = %lf ms", diff);

    LOG_INFO("Buffer Manager Stats after Loading:\n%s", buf_mgr->GetStats().ToString().c_str());
//    LOG_INFO("%sTABLE SIZES%s", peloton::GETINFO_HALF_THICK_LINE.c_str(), peloton::GETINFO_HALF_THICK_LINE.c_str());
//    LOG_INFO("warehouse count = %lu", warehouse_table->GetTupleCount());
//    LOG_INFO("district count  = %lu", district_table->GetTupleCount());
//    LOG_INFO("item count = %lu", item_table->GetTupleCount());
//    LOG_INFO("customer count = %lu", history_table->GetTupleCount());
//    LOG_INFO("history count = %lu", history_table->GetTupleCount());
//    LOG_INFO("stock count = %lu", stock_table->GetTupleCount());
//    LOG_INFO("orders count = %lu", orders_table->GetTupleCount());
//    LOG_INFO("new order count = %lu", new_order_table->GetTupleCount());
//    LOG_INFO("order line count = %lu", order_line_table->GetTupleCount());
}


void CreateTPCCDatabaseFromPersistentStorage(ConcurrentBufferManager *buf_mgr) {
    pid_t tpcc_database_meta_page_pid = TPCCDatabaseMetaPage::meta_page_pid;
    // Restore the pids of the primary index root and version table meta page
    ConcurrentBufferManager::PageAccessor accessor;
    Status s = buf_mgr->Get(tpcc_database_meta_page_pid, accessor, ConcurrentBufferManager::INTENT_READ_FULL);
    assert(s.ok());
    auto slice = accessor.PrepareForRead(0, kPageSize);
    auto tpcc_database_meta_page_desc = accessor.GetPageDesc();
    const auto tpcc_database_meta_page = *reinterpret_cast<TPCCDatabaseMetaPage *>(slice.data());
    accessor.FinishAccess();
    buf_mgr->Put(tpcc_database_meta_page_desc);


    // Warehouse Table
    {
        pid_t primary_index_root_page_pid = tpcc_database_meta_page.m.warehouse_pk_root_page_pid;
        pid_t version_table_meta_page_pid = tpcc_database_meta_page.m.warehouse_vtbl_meta_page_pid;

        auto primary_index = new ClusteredIndex<int32_t, Warehouse>(buf_mgr);
        Status s = primary_index->Init(primary_index_root_page_pid);
        assert(s.ok());
        auto version_table = new PartitionedHeapTable<int32_t, Warehouse>;
        s = version_table->Init(version_table_meta_page_pid, buf_mgr, true);
        assert(s.ok());
        warehouse_table = new WarehouseTable(*primary_index, *version_table);
    }

    // District Table
    {
        pid_t primary_index_root_page_pid = tpcc_database_meta_page.m.district_pk_root_page_pid;
        pid_t version_table_meta_page_pid = tpcc_database_meta_page.m.district_vtbl_meta_page_pid;

        auto primary_index = new ClusteredIndex<District::DistrictKey, District>(buf_mgr);
        Status s = primary_index->Init(primary_index_root_page_pid);
        assert(s.ok());
        auto version_table = new PartitionedHeapTable<District::DistrictKey, District>;
        s = version_table->Init(version_table_meta_page_pid, buf_mgr, true);
        assert(s.ok());
        district_table = new DistrictTable(*primary_index, *version_table);
    }


    // Item Table
    {
        pid_t primary_index_root_page_pid = tpcc_database_meta_page.m.item_pk_root_page_pid;
        pid_t version_table_meta_page_pid = tpcc_database_meta_page.m.item_vtbl_meta_page_pid;

        auto primary_index = new ClusteredIndex<int32_t, Item>(buf_mgr);
        Status s = primary_index->Init(primary_index_root_page_pid);
        assert(s.ok());
        auto version_table = new PartitionedHeapTable<int32_t, Item>;
        s = version_table->Init(version_table_meta_page_pid, buf_mgr, true);
        assert(s.ok());
        item_table = new ItemTable(*primary_index, *version_table);
    }

    // Customer Table
    {
        pid_t primary_index_root_page_pid = tpcc_database_meta_page.m.customer_pk_root_page_pid;
        pid_t version_table_meta_page_pid = tpcc_database_meta_page.m.customer_vtbl_meta_page_pid;

        auto primary_index = new ClusteredIndex<Customer::CustomerKey, Customer>(buf_mgr);
        Status s = primary_index->Init(primary_index_root_page_pid);
        assert(s.ok());
        auto version_table = new PartitionedHeapTable<Customer::CustomerKey, Customer>;
        s = version_table->Init(version_table_meta_page_pid, buf_mgr, true);
        assert(s.ok());
        customer_table = new CustomerTable(*primary_index, *version_table);

        pid_t secondary_index_root_page_pid = tpcc_database_meta_page.m.customer_sk_root_page_pid;
        pid_t secondary_version_table_meta_page_pid = tpcc_database_meta_page.m.customer_sk_vtbl_meta_page_pid;

        auto secondary_index = new ClusteredIndex<CustomerIndexedColumns, CustomerIndexedColumns>(buf_mgr);
        s = secondary_index->Init(secondary_index_root_page_pid);
        assert(s.ok());
        auto secondary_version_table = new PartitionedHeapTable<CustomerIndexedColumns, CustomerIndexedColumns>;
        s = secondary_version_table->Init(secondary_version_table_meta_page_pid, buf_mgr, true);
        assert(s.ok());
        customer_skey_table = new CustomerSKeyTable(*secondary_index, *secondary_version_table);
    }

    // History Table
    {
        pid_t primary_index_root_page_pid = tpcc_database_meta_page.m.history_pk_root_page_pid;
        pid_t version_table_meta_page_pid = tpcc_database_meta_page.m.history_vtbl_meta_page_pid;

        auto primary_index = new ClusteredIndex<int32_t, History>(buf_mgr);
        Status s = primary_index->Init(primary_index_root_page_pid);
        assert(s.ok());
        auto version_table = new PartitionedHeapTable<int32_t, History>;
        s = version_table->Init(version_table_meta_page_pid, buf_mgr, true);
        assert(s.ok());
        history_table = new HistoryTable(*primary_index, *version_table);

        history_id.store(tpcc_database_meta_page.m.history_id_snapshot);
    }


    // Stock Table
    {
        pid_t primary_index_root_page_pid = tpcc_database_meta_page.m.stock_pk_root_page_pid;
        pid_t version_table_meta_page_pid = tpcc_database_meta_page.m.stock_vtbl_meta_page_pid;

        auto primary_index = new ClusteredIndex<Stock::StockKey, Stock>(buf_mgr);
        Status s = primary_index->Init(primary_index_root_page_pid);
        assert(s.ok());
        auto version_table = new PartitionedHeapTable<Stock::StockKey, Stock>();
        s = version_table->Init(version_table_meta_page_pid, buf_mgr, true);
        assert(s.ok());
        stock_table = new StockTable(*primary_index, *version_table);
    }

    // Orders Table
    {
        pid_t primary_index_root_page_pid = tpcc_database_meta_page.m.orders_pk_root_page_pid;
        pid_t version_table_meta_page_pid = tpcc_database_meta_page.m.orders_vtbl_meta_page_pid;

        auto primary_index = new ClusteredIndex<Order::OrderKey, Order>(buf_mgr);
        Status s = primary_index->Init(primary_index_root_page_pid);
        assert(s.ok());
        auto version_table = new PartitionedHeapTable<Order::OrderKey, Order>;
        s = version_table->Init(version_table_meta_page_pid, buf_mgr, true);
        assert(s.ok());
        orders_table = new OrderTable(*primary_index, *version_table);

        pid_t secondary_index_root_page_pid = tpcc_database_meta_page.m.orders_sk_root_page_pid;
        pid_t secondary_version_table_meta_page_pid = tpcc_database_meta_page.m.orders_sk_vtbl_meta_page_pid;

        auto secondary_index = new ClusteredIndex<OrderIndexedColumns, OrderIndexedColumns>(buf_mgr);
        s = secondary_index->Init(secondary_index_root_page_pid);
        assert(s.ok());
        auto secondary_version_table = new PartitionedHeapTable<OrderIndexedColumns, OrderIndexedColumns>;
        s = secondary_version_table->Init(secondary_version_table_meta_page_pid, buf_mgr, true);
        assert(s.ok());
        orders_skey_table = new OrderSKeyTable(*secondary_index, *secondary_version_table);
    }

    // NewOrder Table
    {
        pid_t primary_index_root_page_pid = tpcc_database_meta_page.m.new_order_pk_root_page_pid;
        pid_t version_table_meta_page_pid = tpcc_database_meta_page.m.new_order_vtbl_meta_page_pid;

        auto primary_index = new ClusteredIndex<NewOrder::NewOrderKey, NewOrder>(buf_mgr);
        Status s = primary_index->Init(primary_index_root_page_pid);
        assert(s.ok());
        auto version_table = new PartitionedHeapTable<NewOrder::NewOrderKey, NewOrder>;
        s = version_table->Init(version_table_meta_page_pid, buf_mgr, true);
        assert(s.ok());
        new_order_table = new NewOrderTable(*primary_index, *version_table);
    }

    // OrderLine Table
    {
        pid_t primary_index_root_page_pid = tpcc_database_meta_page.m.order_line_pk_root_page_pid;
        pid_t version_table_meta_page_pid = tpcc_database_meta_page.m.order_line_vtbl_meta_page_pid;

        auto primary_index = new ClusteredIndex<OrderLine::OrderLineKey, OrderLine>(buf_mgr);
        Status s = primary_index->Init(primary_index_root_page_pid);
        assert(s.ok());
        auto version_table = new PartitionedHeapTable<OrderLine::OrderLineKey, OrderLine>;
        s = version_table->Init(version_table_meta_page_pid, buf_mgr, true);
        assert(s.ok());
        order_line_table = new OrderLineTable(*primary_index, *version_table);
    }

    MVTOTransactionManager::GetInstance(buf_mgr)->SetCurrentRowId(tpcc_database_meta_page.m.current_row_id_counter);
    MVTOTransactionManager::GetInstance(buf_mgr)->SetCurrentTid(tpcc_database_meta_page.m.current_tid_counter);
}


void DestroyTPCCDatabase(ConcurrentBufferManager *buf_mgr) {
    pid_t tpcc_database_meta_page_pid = TPCCDatabaseMetaPage::meta_page_pid;
    // Records the pids of the primary/secondary index roots
    ConcurrentBufferManager::PageAccessor accessor;
    Status s = buf_mgr->Get(tpcc_database_meta_page_pid, accessor, ConcurrentBufferManager::INTENT_WRITE_FULL);
    assert(s.ok());
    auto slice = accessor.PrepareForWrite(0, kPageSize);
    auto tpcc_database_meta_page_desc = accessor.GetPageDesc();
    auto tpcc_database_meta_page = reinterpret_cast<TPCCDatabaseMetaPage *>(slice.data());

    tpcc_database_meta_page->m.warehouse_pk_root_page_pid = warehouse_table->GetPrimaryIndex().GetRootPid();
    tpcc_database_meta_page->m.warehouse_vtbl_meta_page_pid = warehouse_table->GetVersionTable().GetMetaPagePid();
    tpcc_database_meta_page->m.district_pk_root_page_pid = district_table->GetPrimaryIndex().GetRootPid();
    tpcc_database_meta_page->m.district_vtbl_meta_page_pid = district_table->GetVersionTable().GetMetaPagePid();
    tpcc_database_meta_page->m.item_pk_root_page_pid = item_table->GetPrimaryIndex().GetRootPid();
    tpcc_database_meta_page->m.item_vtbl_meta_page_pid = item_table->GetVersionTable().GetMetaPagePid();
    tpcc_database_meta_page->m.customer_pk_root_page_pid = customer_table->GetPrimaryIndex().GetRootPid();
    tpcc_database_meta_page->m.customer_vtbl_meta_page_pid = customer_table->GetVersionTable().GetMetaPagePid();
    tpcc_database_meta_page->m.customer_sk_root_page_pid = customer_skey_table->GetPrimaryIndex().GetRootPid();
    tpcc_database_meta_page->m.customer_sk_vtbl_meta_page_pid = customer_skey_table->GetVersionTable().GetMetaPagePid();
    tpcc_database_meta_page->m.history_pk_root_page_pid = history_table->GetPrimaryIndex().GetRootPid();
    tpcc_database_meta_page->m.history_vtbl_meta_page_pid = history_table->GetVersionTable().GetMetaPagePid();
    tpcc_database_meta_page->m.stock_pk_root_page_pid = stock_table->GetPrimaryIndex().GetRootPid();
    tpcc_database_meta_page->m.stock_vtbl_meta_page_pid = stock_table->GetVersionTable().GetMetaPagePid();
    tpcc_database_meta_page->m.orders_pk_root_page_pid = orders_table->GetPrimaryIndex().GetRootPid();
    tpcc_database_meta_page->m.orders_vtbl_meta_page_pid = orders_table->GetVersionTable().GetMetaPagePid();
    tpcc_database_meta_page->m.orders_sk_root_page_pid = orders_skey_table->GetPrimaryIndex().GetRootPid();
    tpcc_database_meta_page->m.orders_sk_vtbl_meta_page_pid = orders_skey_table->GetVersionTable().GetMetaPagePid();
    tpcc_database_meta_page->m.new_order_pk_root_page_pid = new_order_table->GetPrimaryIndex().GetRootPid();
    tpcc_database_meta_page->m.new_order_vtbl_meta_page_pid = new_order_table->GetVersionTable().GetMetaPagePid();
    tpcc_database_meta_page->m.order_line_pk_root_page_pid = order_line_table->GetPrimaryIndex().GetRootPid();
    tpcc_database_meta_page->m.order_line_vtbl_meta_page_pid = order_line_table->GetVersionTable().GetMetaPagePid();


    tpcc_database_meta_page->m.history_id_snapshot = history_id.load();
    tpcc_database_meta_page->m.current_tid_counter = MVTOTransactionManager::GetInstance(buf_mgr)->GetCurrentTid();
    tpcc_database_meta_page->m.current_row_id_counter = MVTOTransactionManager::GetInstance(buf_mgr)->GetCurrentRowId();

    LOG_INFO("warehouse_pk_root_page_pid %lu", tpcc_database_meta_page->m.warehouse_pk_root_page_pid);
    LOG_INFO("warehouse_vtbl_meta_page_pid %lu", tpcc_database_meta_page->m.warehouse_vtbl_meta_page_pid);
    LOG_INFO("district_pk_root_page_pid %lu", tpcc_database_meta_page->m.district_pk_root_page_pid);
    LOG_INFO("district_vtbl_meta_page_pid %lu", tpcc_database_meta_page->m.district_vtbl_meta_page_pid);
    LOG_INFO("item_pk_root_page_pid %lu", tpcc_database_meta_page->m.item_pk_root_page_pid);
    LOG_INFO("item_vtbl_meta_page_pid %lu", tpcc_database_meta_page->m.item_vtbl_meta_page_pid);
    LOG_INFO("customer_pk_root_page_pid %lu", tpcc_database_meta_page->m.customer_pk_root_page_pid);
    LOG_INFO("customer_vtbl_meta_page_pid %lu", tpcc_database_meta_page->m.customer_vtbl_meta_page_pid);
    LOG_INFO("customer_sk_root_page_pid %lu", tpcc_database_meta_page->m.customer_sk_root_page_pid);
    LOG_INFO("customer_sk_vtbl_meta_page_pid %lu", tpcc_database_meta_page->m.customer_sk_vtbl_meta_page_pid);
    LOG_INFO("history_pk_root_page_pid %lu", tpcc_database_meta_page->m.history_pk_root_page_pid);
    LOG_INFO("history_vtbl_meta_page_pid %lu", tpcc_database_meta_page->m.history_vtbl_meta_page_pid);
    LOG_INFO("stock_pk_root_page_pid %lu", tpcc_database_meta_page->m.stock_pk_root_page_pid);
    LOG_INFO("stock_vtbl_meta_page_pid %lu", tpcc_database_meta_page->m.stock_vtbl_meta_page_pid);
    LOG_INFO("orders_pk_root_page_pid %lu", tpcc_database_meta_page->m.orders_pk_root_page_pid);
    LOG_INFO("orders_vtbl_meta_page_pid %lu", tpcc_database_meta_page->m.orders_vtbl_meta_page_pid);
    LOG_INFO("orders_sk_root_page_pid %lu", tpcc_database_meta_page->m.orders_sk_root_page_pid);
    LOG_INFO("orders_sk_vtbl_meta_page_pid %lu", tpcc_database_meta_page->m.orders_sk_vtbl_meta_page_pid);
    LOG_INFO("new_order_pk_root_page_pid %lu", tpcc_database_meta_page->m.new_order_pk_root_page_pid);
    LOG_INFO("new_order_vtbl_meta_page_pid %lu", tpcc_database_meta_page->m.new_order_vtbl_meta_page_pid);
    LOG_INFO("order_line_pk_root_page_pid %lu", tpcc_database_meta_page->m.order_line_pk_root_page_pid);
    LOG_INFO("order_line_vtbl_meta_page_pid %lu", tpcc_database_meta_page->m.order_line_vtbl_meta_page_pid);
    LOG_INFO("history_id_snapshot %lu", history_id.load());
    LOG_INFO("current_tid_counter %lu", MVTOTransactionManager::GetInstance(buf_mgr)->GetCurrentTid());
    LOG_INFO("current_row_id_counter %lu", MVTOTransactionManager::GetInstance(buf_mgr)->GetCurrentRowId());

    accessor.FinishAccess();
    buf_mgr->Put(tpcc_database_meta_page_desc);
//    buf_mgr->Flush(tpcc_database_meta_page_pid, false, false);
//    buf_mgr->Flush(tpcc_database_meta_page_pid, false, false);

    delete &item_table->GetPrimaryIndex();
    delete &item_table->GetVersionTable();
    delete item_table;
    item_table = nullptr;

    delete &warehouse_table->GetPrimaryIndex();
    delete &warehouse_table->GetVersionTable();
    delete warehouse_table;
    warehouse_table = nullptr;

    delete &customer_table->GetPrimaryIndex();
    delete &customer_table->GetVersionTable();
    delete customer_table;
    customer_table = nullptr;

    delete &customer_skey_table->GetPrimaryIndex();
    delete &customer_skey_table->GetVersionTable();
    delete customer_skey_table;
    customer_skey_table = nullptr;

    delete &district_table->GetPrimaryIndex();
    delete &district_table->GetVersionTable();
    delete district_table;
    district_table = nullptr;

    delete &history_table->GetPrimaryIndex();
    delete &history_table->GetVersionTable();
    delete history_table;
    history_table = nullptr;

    delete &new_order_table->GetPrimaryIndex();
    delete &new_order_table->GetVersionTable();
    delete new_order_table;
    new_order_table = nullptr;

    delete &order_line_table->GetPrimaryIndex();
    delete &order_line_table->GetVersionTable();
    delete order_line_table;
    order_line_table = nullptr;

    delete &stock_table->GetPrimaryIndex();
    delete &stock_table->GetVersionTable();
    delete stock_table;
    stock_table = nullptr;

    delete &orders_table->GetPrimaryIndex();
    delete &orders_table->GetVersionTable();
    delete orders_table;
    orders_table = nullptr;

    delete &orders_skey_table->GetPrimaryIndex();
    delete &orders_skey_table->GetVersionTable();
    delete orders_skey_table;
    orders_skey_table = nullptr;
}

}  // namespace tpcc
}  // namespace benchmark
}  // namespace peloton

#ifdef __APPLE__
#pragma clang diagnostic pop
#endif