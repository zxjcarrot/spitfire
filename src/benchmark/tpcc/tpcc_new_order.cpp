//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tpcc_new_order.cpp
//
// Identification: src/main/tpcc/tpcc_new_order.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <ctime>
#include <iostream>
#include <limits>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "benchmark/tpcc/tpcc_configuration.h"
#include "benchmark/tpcc/tpcc_loader.h"
#include "benchmark/tpcc/tpcc_workload.h"
#include "benchmark/tpcc/tpcc_record.h"

#include "engine/executor.h"
#include "util/logger.h"


namespace spitfire {
namespace benchmark {
namespace tpcc {

bool RunNewOrder(const size_t &thread_id, ConcurrentBufferManager *buf_mgr) {
    /*
       "NEW_ORDER": {
       "getWarehouseTaxRate": "SELECT W_TAX FROM WAREHOUSE WHERE W_ID = ?", # w_id
       "getDistrict": "SELECT D_TAX, D_NEXT_O_ID FROM DISTRICT WHERE D_ID = ? AND
       D_W_ID = ?", # d_id, w_id
       "getCustomer": "SELECT C_DISCOUNT, C_LAST, C_CREDIT FROM CUSTOMER WHERE
       C_W_ID = ? AND C_D_ID = ? AND C_ID = ?", # w_id, d_id, c_id
       "incrementNextOrderId": "UPDATE DISTRICT SET D_NEXT_O_ID = ? WHERE D_ID = ?
       AND D_W_ID = ?", # d_next_o_id, d_id, w_id
       "createOrder": "INSERT INTO ORDERS (O_ID, O_D_ID, O_W_ID, O_C_ID,
       O_ENTRY_D, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL) VALUES (?, ?, ?, ?, ?, ?,
       ?, ?)", # d_next_o_id, d_id, w_id, c_id, o_entry_d, o_carrier_id, o_ol_cnt,
       o_all_local
       "createNewOrder": "INSERT INTO NEW_ORDER (NO_O_ID, NO_D_ID, NO_W_ID) VALUES
       (?, ?, ?)", # o_id, d_id, w_id
       "getItemInfo": "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = ?", #
       ol_i_id
       "getStockInfo": "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT,
       S_REMOTE_CNT, S_DIST_%02d FROM STOCK WHERE S_I_ID = ? AND S_W_ID = ?", #
       d_id, ol_i_id, ol_supply_w_id
       "updateStock": "UPDATE STOCK SET S_QUANTITY = ?, S_YTD = ?, S_ORDER_CNT =
       ?, S_REMOTE_CNT = ? WHERE S_I_ID = ? AND S_W_ID = ?", # s_quantity,
       s_order_cnt, s_remote_cnt, ol_i_id, ol_supply_w_id
       "createOrderLine": "INSERT INTO ORDER_LINE (OL_O_ID, OL_D_ID, OL_W_ID,
       OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_DELIVERY_D, OL_QUANTITY, OL_AMOUNT,
       OL_DIST_INFO) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", # o_id, d_id, w_id,
       ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info
       }
     */

    LOG_TRACE("-------------------------------------");

    /////////////////////////////////////////////////////////
    // PREPARE ARGUMENTS
    /////////////////////////////////////////////////////////
    int32_t warehouse_id = GenerateWarehouseId(thread_id);
    int32_t district_id = GetRandomInteger(0, state.districts_per_warehouse - 1);
    int32_t customer_id = GetRandomInteger(0, state.customers_per_district - 1);
    int o_ol_cnt = GetRandomInteger(orders_min_ol_cnt, orders_max_ol_cnt);

    std::vector<int> i_ids, ol_w_ids, ol_qtys;
    bool o_all_local = true;

    for (auto ol_itr = 0; ol_itr < o_ol_cnt; ol_itr++) {
        // in the original TPC-C benchmark, it is possible to read an item that does
        // not exist.
        // for simplicity, we ignore this case.
        // this essentially makes the processing of NewOrder transaction more
        // time-consuming.
        i_ids.push_back(GetRandomInteger(0, state.item_count - 1));
        bool remote = GetRandomBoolean(new_order_remote_txns);
        ol_w_ids.push_back(warehouse_id);

        if (remote == true) {
            ol_w_ids[ol_itr] =
                    GetRandomIntegerExcluding(0, state.warehouse_count - 1, warehouse_id);
            o_all_local = false;
        }

        ol_qtys.push_back(GetRandomInteger(0, order_line_max_ol_quantity));
    }

    //std::vector<expression::AbstractExpression *> runtime_keys;

    /////////////////////////////////////////////////////////
    // BEGIN TRANSACTION
    /////////////////////////////////////////////////////////

    auto txn_manager = MVTOTransactionManager::GetInstance(buf_mgr);

    auto txn = txn_manager->BeginTransaction(thread_id);

//    std::unique_ptr<executor::ExecutorContext> context(
//            new executor::ExecutorContext(txn));
//
//    std::vector<oid_t> item_key_column_ids;
//    std::vector<ExpressionType> item_expr_types;
//    item_key_column_ids.push_back(0);  // I_ID
//    item_expr_types.push_back(ExpressionType::COMPARE_EQUAL);
//
//    std::vector<oid_t> item_column_ids = {2, 3, 4};  // I_NAME, I_PRICE, I_DATA

    for (auto item_id : i_ids) {
        LOG_TRACE("getItemInfo: SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = %d",
                  item_id);

//        std::vector<type::Value> item_key_values;
//
//        item_key_values.push_back(
//                type::ValueFactory::GetIntegerValue(item_id).Copy());
//
//        planner::IndexScanPlan::IndexScanDesc item_index_scan_desc(
//                item_table_pkey_index_oid, item_key_column_ids, item_expr_types,
//                item_key_values, runtime_keys);
//
//        planner::IndexScanPlan item_index_scan_node(
//                item_table, nullptr, item_column_ids, item_index_scan_desc);
//
//        executor::IndexScanExecutor item_index_scan_executor(&item_index_scan_node,
//                                                             context.get());
//

        int32_t key = item_id;
        bool point_lookup = true;
        bool acquire_owner = false;
        auto predicate = [&](const Item &i, bool &should_end_scan) -> bool {
            should_end_scan = true;
            return i.I_ID == item_id;
        };

        IndexScanExecutor<int32_t, Item> executor(*item_table, key, point_lookup, predicate, acquire_owner, txn,
                                                  buf_mgr);
        auto res = executor.Execute();

        if (txn->GetResult() != ResultType::SUCCESS) {
            LOG_TRACE("abort transaction");
            txn_manager->AbortTransaction(txn);
            return false;
        }

        auto &items = executor.GetResults();

        if (items.size() != 1) {
            LOG_ERROR("getItemInfo return size incorrect : %lu",
                      items.size());
            assert(false);
        }
    }

    LOG_TRACE("getWarehouseTaxRate: SELECT W_TAX FROM WAREHOUSE WHERE W_ID = %d",
              warehouse_id);

    double w_tax = 0;
    {
        int vv = 0;
        int32_t warehouse_key = warehouse_id;
        bool point_lookup = true;
        bool acquire_owner = false;
        auto predicate = [&](const Warehouse &w, bool &should_end_scan) -> bool {
            should_end_scan = true;
            auto v = w.W_ID == warehouse_id;
            assert(v);
                //return false;
            return v;
        };

        IndexScanExecutor<int32_t, Warehouse> executor(*warehouse_table, warehouse_key, point_lookup, predicate,
                                                       acquire_owner, txn, buf_mgr);
        auto res = executor.Execute();

        if (txn->GetResult() != ResultType::SUCCESS) {
            LOG_TRACE("abort transaction");
            txn_manager->AbortTransaction(txn);
            return false;
        }

        auto warehouses = executor.GetResults();
        if (executor.GetResults().size() != 1) {
            // Check if aborted
            if (txn->GetResult() != ResultType::SUCCESS) {
                LOG_TRACE("abort transaction");
                txn_manager->AbortTransaction(txn);
                return false;
            }
            assert(executor.GetResults().size() == 1);
        }
        assert(executor.GetResults().size() == 1);

        assert(warehouses.size() == 1);
        w_tax = warehouses[0].W_TAX;
    }

//    std::vector<oid_t> warehouse_key_column_ids;
//    std::vector<ExpressionType> warehouse_expr_types;
//    warehouse_key_column_ids.push_back(0);  // W_ID
//    warehouse_expr_types.push_back(ExpressionType::COMPARE_EQUAL);
//
//    std::vector<type::Value> warehouse_key_values;
//
//    warehouse_key_values.push_back(
//            type::ValueFactory::GetIntegerValue(warehouse_id).Copy());
//
//    planner::IndexScanPlan::IndexScanDesc warehouse_index_scan_desc(
//            warehouse_table_pkey_index_oid, warehouse_key_column_ids,
//            warehouse_expr_types, warehouse_key_values, runtime_keys);
//
//    std::vector<oid_t> warehouse_column_ids = {7};  // W_TAX
//
//    planner::IndexScanPlan warehouse_index_scan_node(warehouse_table, nullptr,
//                                                     warehouse_column_ids,
//                                                     warehouse_index_scan_desc);
//
//    executor::IndexScanExecutor warehouse_index_scan_executor(
//            &warehouse_index_scan_node, context.get());
//
//    auto gwtr_lists_values = ExecuteRead(&warehouse_index_scan_executor);
//
//    if (txn->GetResult() != ResultType::SUCCESS) {
//        LOG_TRACE("abort transaction");
//        txn_manager.AbortTransaction(txn);
//        return false;
//    }
//
//    if (gwtr_lists_values.size() != 1) {
//        LOG_ERROR("getWarehouseTaxRate return size incorrect : %lu",
//                  gwtr_lists_values.size());
//        assert(false);
//    }
//
//    UNUSED_ATTRIBUTE auto w_tax = gwtr_lists_values[0][0];

    LOG_TRACE("w_tax: %s", w_tax.GetInfo().c_str());

    LOG_TRACE(
            "getDistrict: SELECT D_TAX, D_NEXT_O_ID FROM DISTRICT WHERE D_ID = %d "
            "AND D_W_ID = %d",
            district_id, warehouse_id);

    double d_tax = 0;
    int32_t d_next_o_id = 0;

    {
        auto key = District::DistrictKey{
                warehouse_id,
                district_id
        };
        bool point_lookup = true;
        bool acquire_owner = false;
        auto predicate = [&](const District &d, bool &should_end_scan) -> bool {
            should_end_scan = true;
            return d.D_W_ID == warehouse_id && d.D_ID == district_id;
        };

        IndexScanExecutor<District::DistrictKey, District> executor(*district_table, key, point_lookup, predicate,
                                                                    acquire_owner, txn, buf_mgr);
        auto res = executor.Execute();

        if (txn->GetResult() != ResultType::SUCCESS) {
            LOG_TRACE("abort transaction");
            txn_manager->AbortTransaction(txn);
            return false;
        }

        auto districts = executor.GetResults();
        assert(districts.size() == 1);
        d_tax = districts[0].D_TAX;
        d_next_o_id = districts[0].D_NEXT_O_ID;
    }

//    std::vector<oid_t> district_key_column_ids;
//    std::vector<ExpressionType> district_expr_types;
//
//    district_key_column_ids.push_back(0);  // D_ID
//    district_key_column_ids.push_back(1);  // D_W_ID
//    district_expr_types.push_back(ExpressionType::COMPARE_EQUAL);
//    district_expr_types.push_back(ExpressionType::COMPARE_EQUAL);
//
//    std::vector<type::Value> district_key_values;
//    district_key_values.push_back(
//            type::ValueFactory::GetIntegerValue(district_id).Copy());
//    district_key_values.push_back(
//            type::ValueFactory::GetIntegerValue(warehouse_id).Copy());
//
//    planner::IndexScanPlan::IndexScanDesc district_index_scan_desc(
//            district_table_pkey_index_oid, district_key_column_ids,
//            district_expr_types, district_key_values, runtime_keys);
//
//    std::vector<oid_t> district_column_ids = {8, 10};  // D_TAX, D_NEXT_O_ID
//
//    // Create plan node.
//    planner::IndexScanPlan district_index_scan_node(
//            district_table, nullptr, district_column_ids, district_index_scan_desc);
//
//    executor::IndexScanExecutor district_index_scan_executor(
//            &district_index_scan_node, context.get());
//
//    auto gd_lists_values = ExecuteRead(&district_index_scan_executor);
//
//    if (txn->GetResult() != ResultType::SUCCESS) {
//        LOG_TRACE("abort transaction");
//        txn_manager.AbortTransaction(txn);
//        return false;
//    }
//
//    if (gd_lists_values.size() != 1) {
//        LOG_ERROR("getDistrict return size incorrect : %lu",
//                  gd_lists_values.size());
//        assert(false);
//    }
//
//    UNUSED_ATTRIBUTE auto d_tax = gd_lists_values[0][0];
//    UNUSED_ATTRIBUTE auto d_next_o_id = gd_lists_values[0][1];

    LOG_TRACE("d_tax: %s, d_next_o_id: %s", d_tax.GetInfo().c_str(),
              d_next_o_id.GetInfo().c_str());

    LOG_TRACE(
            "getCustomer: SELECT C_DISCOUNT, C_LAST, C_CREDIT FROM CUSTOMER WHERE "
            "C_W_ID = %d AND C_D_ID = %d AND C_ID = %d",
            warehouse_id, district_id, customer_id);

    char c_last[32];
    char c_credit[2];
    double c_discount = 0.0;

    {
        auto key = Customer::CustomerKey{
                warehouse_id,
                district_id,
                customer_id
        };
        bool point_lookup = true;
        bool acquire_owner = false;
        auto predicate = [&](const Customer &c, bool &should_end_scan) -> bool {
            should_end_scan = true;
            return c.C_W_ID == warehouse_id && c.C_D_ID == district_id && c.C_ID == customer_id;
        };

        IndexScanExecutor<Customer::CustomerKey, Customer> executor(*customer_table, key, point_lookup, predicate,
                                                                    acquire_owner, txn, buf_mgr);
        auto res = executor.Execute();

        if (txn->GetResult() != ResultType::SUCCESS) {
            LOG_TRACE("abort transaction");
            txn_manager->AbortTransaction(txn);
            return false;
        }

        auto customers = executor.GetResults();
        assert(customers.size() == 1);
        c_discount = customers[0].C_DISCOUNT;
        memcpy(c_credit, customers[0].C_CREDIT, sizeof(c_credit));
        memcpy(c_last, customers[0].C_LAST, sizeof(c_last));
    }
//    std::vector<oid_t> customer_key_column_ids;
//    std::vector<ExpressionType> customer_expr_types;
//
//    customer_key_column_ids.push_back(0);  // C_ID
//    customer_key_column_ids.push_back(1);  // C_D_ID
//    customer_key_column_ids.push_back(2);  // C_W_ID
//    customer_expr_types.push_back(ExpressionType::COMPARE_EQUAL);
//    customer_expr_types.push_back(ExpressionType::COMPARE_EQUAL);
//    customer_expr_types.push_back(ExpressionType::COMPARE_EQUAL);
//
//    std::vector<type::Value> customer_key_values;
//    customer_key_values.push_back(
//            type::ValueFactory::GetIntegerValue(customer_id).Copy());
//    customer_key_values.push_back(
//            type::ValueFactory::GetIntegerValue(district_id).Copy());
//    customer_key_values.push_back(
//            type::ValueFactory::GetIntegerValue(warehouse_id).Copy());
//
//    planner::IndexScanPlan::IndexScanDesc customer_index_scan_desc(
//            customer_table_pkey_index_oid, customer_key_column_ids,
//            customer_expr_types, customer_key_values, runtime_keys);
//
//    std::vector<oid_t> customer_column_ids = {
//            5, 13, 15};  // C_LAST, C_CREDIT, C_DISCOUNT
//
//    // Create plan node.
//    planner::IndexScanPlan customer_index_scan_node(
//            customer_table, nullptr, customer_column_ids, customer_index_scan_desc);
//
//    executor::IndexScanExecutor customer_index_scan_executor(
//            &customer_index_scan_node, context.get());
//
//    auto gc_lists_values = ExecuteRead(&customer_index_scan_executor);
//
//    if (txn->GetResult() != ResultType::SUCCESS) {
//        LOG_TRACE("abort transaction");
//        txn_manager.AbortTransaction(txn);
//        return false;
//    }
//
//    if (gc_lists_values.size() != 1) {
//        LOG_ERROR("getCustomer return size incorrect : %lu",
//                  gc_lists_values.size());
//        assert(false);
//    }
//
//    UNUSED_ATTRIBUTE auto c_last = gc_lists_values[0][0];
//    UNUSED_ATTRIBUTE auto c_credit = gc_lists_values[0][1];
//    UNUSED_ATTRIBUTE auto c_discount = gc_lists_values[0][2];

    LOG_TRACE("c_last: %s, c_credit: %s, c_discount: %s",
              c_last.GetInfo().c_str(), c_credit.GetInfo().c_str(),
              c_discount.GetInfo().c_str());

//    int district_update_value = type::ValuePeeker::PeekInteger(d_next_o_id) + 1;
    int district_update_value = d_next_o_id + 1;
    LOG_TRACE("district update value = %d", district_update_value);

    LOG_TRACE(
            "incrementNextOrderId: UPDATE DISTRICT SET D_NEXT_O_ID = %d WHERE D_ID = "
            "%d AND D_W_ID = %d",
            district_update_value, district_id, warehouse_id);

    {
        auto key = District::DistrictKey{warehouse_id,
                                         district_id};
        auto predicate = [&](const District &d) -> bool {
            return d.D_ID == district_id && d.D_W_ID == warehouse_id;
        };
        auto updater = [&](District &d) {
            d.D_NEXT_O_ID = district_update_value;
        };
        PointUpdateExecutor<District::DistrictKey, District> executor(*district_table, key,
                                                                      predicate, updater, txn, buf_mgr);
        auto res = executor.Execute();
        if (txn->GetResult() != ResultType::SUCCESS) {
            LOG_TRACE("abort transaction");
            txn_manager->AbortTransaction(txn);
            return false;
        }

    }
//    std::vector<oid_t> district_update_column_ids = {10};  // D_NEXT_O_ID
//
//    std::vector<type::Value> district_update_key_values;
//    district_update_key_values.push_back(
//            type::ValueFactory::GetIntegerValue(district_id).Copy());
//    district_update_key_values.push_back(
//            type::ValueFactory::GetIntegerValue(warehouse_id).Copy());
//
//    planner::IndexScanPlan::IndexScanDesc district_update_index_scan_desc(
//            district_table_pkey_index_oid, district_key_column_ids,
//            district_expr_types, district_update_key_values, runtime_keys);
//
//    // Create plan node.
//    planner::IndexScanPlan district_update_index_scan_node(
//            district_table, nullptr, district_update_column_ids,
//            district_update_index_scan_desc);
//
//    executor::IndexScanExecutor district_update_index_scan_executor(
//            &district_update_index_scan_node, context.get());
//
//    TargetList district_target_list;
//    DirectMapList district_direct_map_list;
//
//    // Update the last attribute
//    for (oid_t col_itr = 0; col_itr < 10; col_itr++) {
//        district_direct_map_list.emplace_back(col_itr,
//                                              std::pair<oid_t, oid_t>(0, col_itr));
//    }
//    type::Value district_update_val =
//            type::ValueFactory::GetIntegerValue(district_update_value).Copy();
//
//    planner::DerivedAttribute attribute{
//            expression::ExpressionUtil::ConstantValueFactory(district_update_val)};
//    district_target_list.emplace_back(10, attribute);
//
//    std::unique_ptr<const planner::ProjectInfo> district_project_info(
//            new planner::ProjectInfo(std::move(district_target_list),
//                                     std::move(district_direct_map_list)));
//    planner::UpdatePlan district_update_node(district_table,
//                                             std::move(district_project_info));
//
//    executor::UpdateExecutor district_update_executor(&district_update_node,
//                                                      context.get());
//
//    district_update_executor.AddChild(&district_update_index_scan_executor);
//
//    ExecuteUpdate(&district_update_executor);
//
//    if (txn->GetResult() != ResultType::SUCCESS) {
//        LOG_TRACE("abort transaction");
//        txn_manager.AbortTransaction(txn);
//        return false;
//    }

    LOG_TRACE(
            "createOrder: INSERT INTO ORDERS (O_ID, O_D_ID, O_W_ID, O_C_ID, "
            "O_ENTRY_D, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL)");

    {
        Order o;
        o.O_ID = d_next_o_id;
        o.O_C_ID = customer_id;
        o.O_D_ID = district_id;
        o.O_W_ID = warehouse_id;
        o.O_ENTRY_D = 1;
        o.O_CARRIER_ID = 0;
        o.O_OL_CNT = o_ol_cnt;
        o.O_ALL_LOCAL = o_all_local;
        InsertExecutor<Order::OrderKey, Order> executor(*orders_table, o, txn, buf_mgr);

        auto res = executor.Execute();
        if (txn->GetResult() != ResultType::SUCCESS) {
            LOG_TRACE(
                    "abort transaction when inserting order table, thread_id = %d, d_id = "
                    "%d, next_o_id = %d",
                    (int) thread_id, (int) district_id,
                    (int) type::ValuePeeker::PeekInteger(d_next_o_id));
            txn_manager->AbortTransaction(txn);
            return false;
        } else {
            LOG_TRACE(
                    "successfully insert order table, thread_id = %d, d_id = %d, next_o_id "
                    "= %d",
                    (int) thread_id, (int) district_id,
                    (int) type::ValuePeeker::PeekInteger(d_next_o_id));
        }

        OrderIndexedColumns oic(o);
        InsertExecutor<OrderIndexedColumns, OrderIndexedColumns> oic_executor(*orders_skey_table, oic, txn, buf_mgr);

        res = oic_executor.Execute();
        if (txn->GetResult() != ResultType::SUCCESS) {
            LOG_TRACE(
                    "abort transaction when inserting secondary index, thread_id = %d, d_id = "
                    "%d, next_o_id = %d",
                    (int) thread_id, (int) district_id,
                    (int) type::ValuePeeker::PeekInteger(d_next_o_id));
            txn_manager->AbortTransaction(txn);
            return false;
        } else {
            LOG_TRACE(
                    "successfully insert secondary index for order, thread_id = %d, d_id = %d, next_o_id "
                    "= %d",
                    (int) thread_id, (int) district_id,
                    (int) type::ValuePeeker::PeekInteger(d_next_o_id));
        }

    }
//    std::unique_ptr<storage::Tuple> orders_tuple(
//            new storage::Tuple(orders_table->GetSchema(), true));
//
//    // O_ID
//    orders_tuple->SetValue(0, type::ValueFactory::GetIntegerValue(
//            type::ValuePeeker::PeekInteger(d_next_o_id)),
//                           nullptr);
//    // O_C_ID
//    orders_tuple->SetValue(1, type::ValueFactory::GetIntegerValue(customer_id),
//                           nullptr);
//    // O_D_ID
//    orders_tuple->SetValue(2, type::ValueFactory::GetIntegerValue(district_id),
//                           nullptr);
//    // O_W_ID
//    orders_tuple->SetValue(3, type::ValueFactory::GetIntegerValue(warehouse_id),
//                           nullptr);
//    // O_ENTRY_D
//    // auto o_entry_d = GetTimeStamp();
//    orders_tuple->SetValue(4, type::ValueFactory::GetTimestampValue(1), nullptr);
//    // O_CARRIER_ID
//    orders_tuple->SetValue(5, type::ValueFactory::GetIntegerValue(0), nullptr);
//    // O_OL_CNT
//    orders_tuple->SetValue(6, type::ValueFactory::GetIntegerValue(o_ol_cnt),
//                           nullptr);
//    // O_ALL_LOCAL
//    orders_tuple->SetValue(7, type::ValueFactory::GetIntegerValue(o_all_local),
//                           nullptr);
//
//    planner::InsertPlan orders_node(orders_table, std::move(orders_tuple));
//    executor::InsertExecutor orders_executor(&orders_node, context.get());
//    orders_executor.Execute();
//
//    if (txn->GetResult() != ResultType::SUCCESS) {
//        LOG_TRACE(
//                "abort transaction when inserting order table, thread_id = %d, d_id = "
//                "%d, next_o_id = %d",
//                (int)thread_id, (int)district_id,
//                (int)type::ValuePeeker::PeekInteger(d_next_o_id));
//        txn_manager.AbortTransaction(txn);
//        return false;
//    } else {
//        LOG_TRACE(
//                "successfully insert order table, thread_id = %d, d_id = %d, next_o_id "
//                "= %d",
//                (int)thread_id, (int)district_id,
//                (int)type::ValuePeeker::PeekInteger(d_next_o_id));
//    }

    LOG_TRACE(
            "createNewOrder: INSERT INTO NEW_ORDER (NO_O_ID, NO_D_ID, NO_W_ID) "
            "VALUES (?, ?, ?)");
    {
        NewOrder no;
        no.NO_W_ID = warehouse_id;
        no.NO_D_ID = district_id;
        no.NO_O_ID = d_next_o_id;
        InsertExecutor<NewOrder::NewOrderKey, NewOrder> executor(*new_order_table, no, txn, buf_mgr);

        auto res = executor.Execute();
        if (txn->GetResult() != ResultType::SUCCESS) {
            LOG_TRACE("abort transaction when inserting new order table");
            txn_manager->AbortTransaction(txn);
            return false;
        }
    }

//    std::unique_ptr<storage::Tuple> new_order_tuple(
//            new storage::Tuple(new_order_table->GetSchema(), true));
//
//    // NO_O_ID
//    new_order_tuple->SetValue(0, type::ValueFactory::GetIntegerValue(
//            type::ValuePeeker::PeekInteger(d_next_o_id)),
//                              nullptr);
//    // NO_D_ID
//    new_order_tuple->SetValue(1, type::ValueFactory::GetIntegerValue(district_id),
//                              nullptr);
//    // NO_W_ID
//    new_order_tuple->SetValue(
//            2, type::ValueFactory::GetIntegerValue(warehouse_id), nullptr);
//
//    planner::InsertPlan new_order_node(new_order_table,
//                                       std::move(new_order_tuple));
//    executor::InsertExecutor new_order_executor(&new_order_node, context.get());
//    new_order_executor.Execute();
//
//    if (txn->GetResult() != ResultType::SUCCESS) {
//        LOG_TRACE("abort transaction when inserting new order table");
//        txn_manager.AbortTransaction(txn);
//        return false;
//    }

//    std::vector<oid_t> stock_key_column_ids;
//    std::vector<ExpressionType> stock_expr_types;
//
//    stock_key_column_ids.push_back(0);  // S_I_ID
//    stock_key_column_ids.push_back(1);  // S_W_ID
//    stock_expr_types.push_back(ExpressionType::COMPARE_EQUAL);
//    stock_expr_types.push_back(ExpressionType::COMPARE_EQUAL);
//
//    auto stock_pkey_index =
//            stock_table->GetIndexWithOid(stock_table_pkey_index_oid);
//
//    // S_QUANTITY, S_DIST_%02d, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DATA
//    std::vector<oid_t> stock_column_ids = {2, oid_t(3 + district_id), 13, 14, 15,
//                                           16};
//
//    std::vector<oid_t> stock_update_column_ids = {
//            2, 13, 14, 15};  // S_QUANTITY, S_YTD, S_ORDER_CNT, S_REMOTE_CNT
//
    for (size_t i = 0; i < i_ids.size(); ++i) {
        int item_id = i_ids.at(i);
        int ol_w_id = ol_w_ids.at(i);
        int ol_qty = ol_qtys.at(i);

        LOG_TRACE(
                "getStockInfo: SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, "
                "S_REMOTE_CNT, S_DIST_? FROM STOCK WHERE S_I_ID = %d AND S_W_ID = %d",
                item_id, ol_w_id);

        auto key = Stock::StockKey{
                ol_w_id,
                item_id,
        };
        bool point_lookup = true;
        bool acquire_owner = false;
        auto predicate = [&](const Stock &s, bool &should_end_scan) -> bool {
            should_end_scan = true;
            return s.S_W_ID == ol_w_id && s.S_I_ID == item_id;
        };

        IndexScanExecutor<Stock::StockKey, Stock> executor(*stock_table, key, point_lookup, predicate, acquire_owner,
                                                           txn, buf_mgr);
        auto res = executor.Execute();

        if (txn->GetResult() != ResultType::SUCCESS) {
            LOG_TRACE("abort transaction");
            txn_manager->AbortTransaction(txn);
            return false;
        }

        auto stocks = executor.GetResults();
        assert(stocks.size() == 1);


//        std::vector<type::Value> stock_key_values;
//
//        stock_key_values.push_back(
//                type::ValueFactory::GetIntegerValue(item_id).Copy());
//        stock_key_values.push_back(
//                type::ValueFactory::GetIntegerValue(ol_w_id).Copy());
//
//        planner::IndexScanPlan::IndexScanDesc stock_index_scan_desc(
//                stock_table_pkey_index_oid, stock_key_column_ids, stock_expr_types,
//                stock_key_values, runtime_keys);
//
//        std::vector<type::Value> stock_update_key_values;
//
//        stock_update_key_values.push_back(
//                type::ValueFactory::GetIntegerValue(item_id).Copy());
//        stock_update_key_values.push_back(
//                type::ValueFactory::GetIntegerValue(ol_w_id).Copy());
//
//        planner::IndexScanPlan::IndexScanDesc stock_update_index_scan_desc(
//                stock_table_pkey_index_oid, stock_key_column_ids, stock_expr_types,
//                stock_update_key_values, runtime_keys);
//
//        // Create plan node.
//        planner::IndexScanPlan stock_index_scan_node(
//                stock_table, nullptr, stock_column_ids, stock_index_scan_desc);
//
//        executor::IndexScanExecutor stock_index_scan_executor(
//                &stock_index_scan_node, context.get());
//
//        auto gsi_lists_values = ExecuteRead(&stock_index_scan_executor);
//
//        if (txn->GetResult() != ResultType::SUCCESS) {
//            LOG_TRACE("abort transaction");
//            txn_manager.AbortTransaction(txn);
//            return false;
//        }
//
//        if (gsi_lists_values.size() != 1) {
//            LOG_ERROR("getStockInfo return size incorrect : %lu",
//                      gsi_lists_values.size());
//            assert(false);
//        }

        //int s_quantity = type::ValuePeeker::PeekInteger(gsi_lists_values[0][0]);

        int s_quantity = stocks[0].S_QUANTITY;

        if (s_quantity >= ol_qty + 10) {
            s_quantity = s_quantity - ol_qty;
        } else {
            s_quantity = s_quantity + 91 - ol_qty;
        }

        //type::Value s_data = gsi_lists_values[0][1];
        char s_data[64];
        memcpy(s_data, stocks[0].S_DATA, sizeof(s_data));


        //int s_ytd = type::ValuePeeker::PeekInteger(gsi_lists_values[0][2]) + ol_qty;

        int s_ytd = stocks[0].S_YTD;

        //int s_order_cnt =
        //        type::ValuePeeker::PeekInteger(gsi_lists_values[0][3]) + 1;

        int s_order_cnt = stocks[0].S_ORDER_CNT;

        //int s_remote_cnt = type::ValuePeeker::PeekInteger(gsi_lists_values[0][4]);
        int s_remote_cnt = stocks[0].S_REMOTE_CNT;

        if (ol_w_id != warehouse_id) {
            s_remote_cnt += 1;
        }

        LOG_TRACE(
                "updateStock: UPDATE STOCK SET S_QUANTITY = ?, S_YTD = ?, S_ORDER_CNT "
                "= ?, S_REMOTE_CNT = ? WHERE S_I_ID = ? AND S_W_ID = ?");


        auto stock_key = Stock::StockKey{ol_w_id,
                                         item_id};
        auto stock_predicate = [&](const Stock &s) -> bool {
            return s.S_W_ID == ol_w_id && s.S_I_ID == item_id;
        };
        auto stock_updater = [&](Stock &d) {
            d.S_QUANTITY = s_quantity;
            d.S_YTD = s_ytd;
            d.S_ORDER_CNT = s_order_cnt;
            d.S_REMOTE_CNT = s_remote_cnt;
        };
        PointUpdateExecutor<Stock::StockKey, Stock> stock_update_executor(*stock_table, stock_key,
                                                                          stock_predicate, stock_updater, txn, buf_mgr);
        res = executor.Execute();
        if (txn->GetResult() != ResultType::SUCCESS) {
            LOG_TRACE("abort transaction");
            txn_manager->AbortTransaction(txn);
            return false;
        }


//        // Create plan node.
//        planner::IndexScanPlan stock_update_index_scan_node(
//                stock_table, nullptr, stock_update_column_ids,
//                stock_update_index_scan_desc);
//
//        executor::IndexScanExecutor stock_update_index_scan_executor(
//                &stock_update_index_scan_node, context.get());
//
//        TargetList stock_target_list;
//        DirectMapList stock_direct_map_list;
//
//        // Update the last attribute
//        for (oid_t col_itr = 0; col_itr < 17; col_itr++) {
//            if (col_itr != 2 && col_itr != 13 && col_itr != 14 && col_itr != 15) {
//                stock_direct_map_list.emplace_back(col_itr,
//                                                   std::pair<oid_t, oid_t>(0, col_itr));
//            }
//        }
//        planner::DerivedAttribute s_quantity_attr{
//                expression::ExpressionUtil::ConstantValueFactory(
//                        type::ValueFactory::GetIntegerValue(s_quantity))};
//
//        planner::DerivedAttribute s_ytd_attr{
//                expression::ExpressionUtil::ConstantValueFactory(
//                        type::ValueFactory::GetIntegerValue(s_ytd))};
//
//        planner::DerivedAttribute s_order_cnt_attr{
//                expression::ExpressionUtil::ConstantValueFactory(
//                        type::ValueFactory::GetIntegerValue(s_order_cnt))};
//
//        planner::DerivedAttribute s_remote_cnt_attr{
//                expression::ExpressionUtil::ConstantValueFactory(
//                        type::ValueFactory::GetIntegerValue(s_remote_cnt))};
//
//        stock_target_list.emplace_back(2, s_quantity_attr);
//        stock_target_list.emplace_back(13, s_ytd_attr);
//        stock_target_list.emplace_back(14, s_order_cnt_attr);
//        stock_target_list.emplace_back(15, s_remote_cnt_attr);
//
//        std::unique_ptr<const planner::ProjectInfo> stock_project_info(
//                new planner::ProjectInfo(std::move(stock_target_list),
//                                         std::move(stock_direct_map_list)));
//        planner::UpdatePlan stock_update_node(stock_table,
//                                              std::move(stock_project_info));
//
//        executor::UpdateExecutor stock_update_executor(&stock_update_node,
//                                                       context.get());
//
//        stock_update_executor.AddChild(&stock_update_index_scan_executor);
//
//        ExecuteUpdate(&stock_update_executor);
//
//        if (txn->GetResult() != ResultType::SUCCESS) {
//            LOG_TRACE("abort transaction");
//            txn_manager.AbortTransaction(txn);
//            return false;
//        }

        // the original benchmark requires check constraints.
        // however, we ignored here.
        // it does not influence the performance.
        // if i_data.find(constants.ORIGINAL_STRING) != -1 and
        // s_data.find(constants.ORIGINAL_STRING) != -1:
        // brand_generic = 'B'
        // else:
        // brand_generic = 'G'

        LOG_TRACE(
                "createOrderLine: INSERT INTO ORDER_LINE (OL_O_ID, OL_D_ID, OL_W_ID, "
                "OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_DELIVERY_D, OL_QUANTITY, "
                "OL_AMOUNT, OL_DIST_INFO) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

        {
            OrderLine ol;
            ol.OL_O_ID = d_next_o_id;
            ol.OL_D_ID = district_id;
            ol.OL_W_ID = warehouse_id;
            ol.OL_NUMBER = i;
            ol.OL_I_ID = item_id;
            ol.OL_SUPPLY_W_ID = ol_w_id;
            ol.OL_DELIVERY_D = 1;
            ol.OL_QUANTITY = ol_qty;
            ol.OL_AMOUNT = 0;
            memcpy(ol.OL_DIST_INFO, s_data, sizeof(ol.OL_DIST_INFO));
            InsertExecutor<OrderLine::OrderLineKey, OrderLine> order_line_insert_executor(*order_line_table, ol, txn,
                                                                                          buf_mgr);
            auto res = executor.Execute();
            if (txn->GetResult() != ResultType::SUCCESS) {
                LOG_TRACE("abort transaction when inserting order line table");
                txn_manager->AbortTransaction(txn);
                return false;
            }

        }
//        std::unique_ptr<storage::Tuple> order_line_tuple(
//                new storage::Tuple(order_line_table->GetSchema(), true));
//
//        // OL_O_ID
//        order_line_tuple->SetValue(0,
//                                   type::ValueFactory::GetIntegerValue(
//                                           type::ValuePeeker::PeekInteger(d_next_o_id)),
//                                   nullptr);
//        // OL_D_ID
//        order_line_tuple->SetValue(
//                1, type::ValueFactory::GetIntegerValue(district_id), nullptr);
//        // OL_W_ID
//        order_line_tuple->SetValue(
//                2, type::ValueFactory::GetIntegerValue(warehouse_id), nullptr);
//        // OL_NUMBER
//        order_line_tuple->SetValue(3, type::ValueFactory::GetIntegerValue(i),
//                                   nullptr);
//        // OL_I_ID
//        order_line_tuple->SetValue(4, type::ValueFactory::GetIntegerValue(item_id),
//                                   nullptr);
//        // OL_SUPPLY_W_ID
//        order_line_tuple->SetValue(5, type::ValueFactory::GetIntegerValue(ol_w_id),
//                                   nullptr);
//        // OL_DELIVERY_D
//        order_line_tuple->SetValue(6, type::ValueFactory::GetTimestampValue(1),
//                                   nullptr);
//        // OL_QUANTITY
//        order_line_tuple->SetValue(7, type::ValueFactory::GetIntegerValue(ol_qty),
//                                   nullptr);
//        // OL_AMOUNT
//        // TODO: workaround!!! I don't know how to get float from Value.
//        order_line_tuple->SetValue(8, type::ValueFactory::GetDecimalValue(0),
//                                   nullptr);
//        // OL_DIST_INFO
//        order_line_tuple->SetValue(9, s_data, nullptr);
//
//        planner::InsertPlan order_line_node(order_line_table,
//                                            std::move(order_line_tuple));
//        executor::InsertExecutor order_line_executor(&order_line_node,
//                                                     context.get());
//        order_line_executor.Execute();
//
//        if (txn->GetResult() != ResultType::SUCCESS) {
//            LOG_TRACE("abort transaction when inserting order line table");
//            txn_manager.AbortTransaction(txn);
//            return false;
//        }
    }

    // transaction passed execution.
    assert(txn->GetResult() == ResultType::SUCCESS);

    auto result = txn_manager->CommitTransaction(txn);

    if (result == ResultType::SUCCESS) {
        // transaction passed commitment.
        LOG_TRACE("commit txn, thread_id = %d, d_id = %d, next_o_id = %d",
                  (int) thread_id, (int) district_id,
                  (int) type::ValuePeeker::PeekInteger(d_next_o_id));
        return true;

    } else {
        // transaction failed commitment.
        assert(result == ResultType::ABORTED ||
               result == ResultType::FAILURE);
        LOG_TRACE("abort txn, thread_id = %d, d_id = %d, next_o_id = %d",
                  (int) thread_id, (int) district_id,
                  (int) type::ValuePeeker::PeekInteger(d_next_o_id));
        return false;
    }
}
}
}
}
