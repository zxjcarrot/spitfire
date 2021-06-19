//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tpcc_order_status.cpp
//
// Identification: src/main/tpcc/tpcc_order_status.cpp
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
#include <benchmark/tpcc/tpcc_record.h>
#include <engine/executor.h>

#include "engine/txn.h"
#include "util/logger.h"

#include "benchmark/tpcc/tpcc_configuration.h"
#include "benchmark/tpcc/tpcc_loader.h"
#include "benchmark/tpcc/tpcc_workload.h"


namespace spitfire {
namespace benchmark {
namespace tpcc {

bool RunOrderStatus(const size_t &thread_id, ConcurrentBufferManager *buf_mgr) {
    /*
      "ORDER_STATUS": {
      "getCustomerByCustomerId": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST,
      C_BALANCE FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?", #
      w_id, d_id, c_id
      "getCustomersByLastName": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE
      FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_LAST = ? ORDER BY
      C_FIRST", # w_id, d_id, c_last
      "getLastOrder": "SELECT O_ID, O_CARRIER_ID, O_ENTRY_D FROM ORDERS WHERE
      O_W_ID = ? AND O_D_ID = ? AND O_C_ID = ? ORDER BY O_ID DESC LIMIT 1", #
      w_id, d_id, c_id
      "getOrderLines": "SELECT OL_SUPPLY_W_ID, OL_I_ID, OL_QUANTITY, OL_AMOUNT,
      OL_DELIVERY_D FROM ORDER_LINE WHERE OL_W_ID = ? AND OL_D_ID = ? AND OL_O_ID
      = ?", # w_id, d_id, o_id
      }
     */

    auto txn_manager = MVTOTransactionManager::GetInstance(buf_mgr);
    auto txn = txn_manager->BeginTransaction(thread_id);

//    std::unique_ptr<executor::ExecutorContext> context(
//            new executor::ExecutorContext(txn));

    // Generate w_id, d_id, c_id, c_last
    // int w_id = GetRandomInteger(0, state.warehouse_count - 1);
    int w_id = GenerateWarehouseId(thread_id);
    int d_id = GetRandomInteger(0, state.districts_per_warehouse - 1);

    int c_id = -1;
    std::string c_last;

    if (GetRandomInteger(1, 100) <= 60) {
        c_last = GetRandomLastName(state.customers_per_district);
    } else {
        c_id = GetNURand(1023, 0, state.customers_per_district - 1);
    }

    // Run queries
    if (c_id != -1) {
        LOG_TRACE(
                "getCustomerByCustomerId: SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, "
                "C_BALANCE FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?  "
                "# w_id, d_id, c_id");
        {
            auto key = Customer::CustomerKey{w_id,
                                             d_id,
                                             c_id};
            auto point_lookup = true;
            auto acquire_owner = false;
            auto predicate = [&](const Customer &c, bool &should_end_scan) -> bool {
                should_end_scan = true;
                return c.C_W_ID == w_id && c.C_D_ID == d_id && c.C_ID == c_id;
            };

            IndexScanExecutor<Customer::CustomerKey, Customer> executor(*customer_table, key, point_lookup,
                                                                        predicate, acquire_owner, txn, buf_mgr);
            auto res = executor.Execute();

            if (txn->GetResult() != ResultType::SUCCESS) {
                txn_manager->AbortTransaction(txn);
                return false;
            }

            auto customers = executor.GetResults();
            if (customers.size() == 0) {
                LOG_ERROR("wrong result size : %lu", customers.size());
                assert(false);
            }
        }
//        // Construct index scan executor
//        std::vector<oid_t> customer_column_ids = {COL_IDX_C_ID, COL_IDX_C_FIRST,
//                                                  COL_IDX_C_MIDDLE, COL_IDX_C_LAST,
//                                                  COL_IDX_C_BALANCE};
//        std::vector<oid_t> customer_key_column_ids = {COL_IDX_C_W_ID,
//                                                      COL_IDX_C_D_ID, COL_IDX_C_ID};
//        std::vector<ExpressionType> customer_expr_types;
//        std::vector<type::Value> customer_key_values;
//        std::vector<expression::AbstractExpression *> runtime_keys;
//
//        customer_expr_types.push_back(ExpressionType::COMPARE_EQUAL);
//        customer_key_values.push_back(
//                type::ValueFactory::GetIntegerValue(w_id).Copy());
//        customer_expr_types.push_back(ExpressionType::COMPARE_EQUAL);
//        customer_key_values.push_back(
//                type::ValueFactory::GetIntegerValue(d_id).Copy());
//        customer_expr_types.push_back(ExpressionType::COMPARE_EQUAL);
//        customer_key_values.push_back(
//                type::ValueFactory::GetIntegerValue(c_id).Copy());
//
//        planner::IndexScanPlan::IndexScanDesc customer_index_scan_desc(
//                customer_table_pkey_index_oid, customer_key_column_ids,
//                customer_expr_types, customer_key_values, runtime_keys);
//
//        auto predicate = nullptr;
//        planner::IndexScanPlan customer_index_scan_node(customer_table, predicate,
//                                                        customer_column_ids,
//                                                        customer_index_scan_desc);
//
//        executor::IndexScanExecutor customer_index_scan_executor(
//                &customer_index_scan_node, context.get());
//
//        auto result = ExecuteRead(&customer_index_scan_executor);
//        if (txn->GetResult() != ResultType::SUCCESS) {
//            txn_manager.AbortTransaction(txn);
//            return false;
//        }
//
//        if (result.size() == 0) {
//            LOG_ERROR("wrong result size : %lu", result.size());
//            assert(false);
//        }
//        if (result[0].size() == 0) {
//            LOG_ERROR("wrong result[0] size : %lu", result[0].size());
//            assert(false);
//        }
    } else {
        LOG_TRACE(
                "getCustomersByLastName: SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, "
                "C_BALANCE FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_LAST = "
                "? ORDER BY C_FIRST, # w_id, d_id, c_last");

        {
            // Finc c_id using secondary index
            auto key = CustomerIndexedColumns();
            key.C_W_ID = w_id;
            key.C_D_ID = d_id;
            key.C_ID = -1;
            strcpy(key.C_LAST, c_last.c_str());
            auto point_lookup = false;
            auto acquire_owner = false;
            auto predicate = [&](const CustomerIndexedColumns &c, bool &should_end_scan) -> bool {
                if (c.C_W_ID == w_id && c.C_D_ID == d_id && c.C_LAST == c_last) {
                    return true;
                }
                should_end_scan = true;
                return false;
            };

            IndexScanExecutor<CustomerIndexedColumns, CustomerIndexedColumns> executor(*customer_skey_table, key,
                                                                                       point_lookup, predicate,
                                                                                       acquire_owner, txn, buf_mgr);

            auto res = executor.Execute();
            if (txn->GetResult() != ResultType::SUCCESS) {
                txn_manager->AbortTransaction(txn);
                return false;
            }
            auto customers = executor.GetResults();
            sort(customers.begin(), customers.end(),
                 [](const CustomerIndexedColumns &lhs, const CustomerIndexedColumns &rhs) {
                     return std::string(lhs.C_LAST) < std::string(rhs.C_LAST);
                 });
            assert(customers.size() > 0);
            // Get the middle one
            size_t name_count = customers.size();
            auto &customer = customers[name_count / 2];
            c_id = customer.C_ID;
        }
//        // Construct index scan executor
//        std::vector<oid_t> customer_column_ids = {COL_IDX_C_ID, COL_IDX_C_FIRST,
//                                                  COL_IDX_C_MIDDLE, COL_IDX_C_LAST,
//                                                  COL_IDX_C_BALANCE};
//        std::vector<oid_t> customer_key_column_ids = {
//                COL_IDX_C_W_ID, COL_IDX_C_D_ID, COL_IDX_C_LAST};
//        std::vector<ExpressionType> customer_expr_types;
//        std::vector<type::Value> customer_key_values;
//        std::vector<expression::AbstractExpression *> runtime_keys;
//
//        customer_expr_types.push_back(ExpressionType::COMPARE_EQUAL);
//        customer_key_values.push_back(
//                type::ValueFactory::GetIntegerValue(w_id).Copy());
//        customer_expr_types.push_back(ExpressionType::COMPARE_EQUAL);
//        customer_key_values.push_back(
//                type::ValueFactory::GetIntegerValue(d_id).Copy());
//        customer_expr_types.push_back(ExpressionType::COMPARE_EQUAL);
//        customer_key_values.push_back(
//                type::ValueFactory::GetVarcharValue(c_last).Copy());
//
//        planner::IndexScanPlan::IndexScanDesc customer_index_scan_desc(
//                customer_table_skey_index_oid, customer_key_column_ids,
//                customer_expr_types, customer_key_values, runtime_keys);
//
//        auto predicate = nullptr;
//        planner::IndexScanPlan customer_index_scan_node(customer_table, predicate,
//                                                        customer_column_ids,
//                                                        customer_index_scan_desc);
//
//        executor::IndexScanExecutor customer_index_scan_executor(
//                &customer_index_scan_node, context.get());
//
//        // Construct order by executor
//        std::vector<oid_t> sort_keys = {1};
//        std::vector<bool> descend_flags = {false};
//        std::vector<oid_t> output_columns = {0, 1, 2, 3, 4};
//
//        planner::OrderByPlan customer_order_by_node(sort_keys, descend_flags,
//                                                    output_columns);
//
//        executor::OrderByExecutor customer_order_by_executor(
//                &customer_order_by_node, context.get());
//
//        customer_order_by_executor.AddChild(&customer_index_scan_executor);
//
//        auto result = ExecuteRead(&customer_order_by_executor);
//        if (txn->GetResult() != ResultType::SUCCESS) {
//            txn_manager.AbortTransaction(txn);
//            return false;
//        }
//
//        assert(result.size() > 0);
//        // Get the middle one
//        size_t name_count = result.size();
//        auto &customer = result[name_count / 2];
//        assert(customer.size() > 0);
//        c_id = type::ValuePeeker::PeekInteger(customer[0]);
    }

    if (c_id < 0) {
        LOG_ERROR("wrong c_id");
        assert(false);
    }

    LOG_TRACE(
            "getLastOrder: SELECT O_ID, O_CARRIER_ID, O_ENTRY_D FROM ORDERS WHERE "
            "O_W_ID = ? AND O_D_ID = ? AND O_C_ID = ? ORDER BY O_ID DESC LIMIT 1, # "
            "w_id, d_id, c_id");

    int32_t last_o_id = -1;
    {
        // Find o_id using secondary index
        auto oic = OrderIndexedColumns();
        oic.O_W_ID = w_id;
        oic.O_D_ID = d_id;
        oic.O_C_ID = c_id;
        oic.O_ID = -1;
        auto point_lookup = false;
        auto acquire_owner = false;

        // Hack: we get the last order id in the predicate.
        auto predicate = [&](const OrderIndexedColumns &oic, bool &should_end_scan) -> bool {
            if (oic.O_W_ID == w_id && oic.O_D_ID == d_id && oic.O_C_ID == c_id) {
                return true;
            }
            should_end_scan = true;
            return false;
        };

        IndexScanExecutor<OrderIndexedColumns, OrderIndexedColumns> executor(*orders_skey_table, oic, point_lookup,
                                                                             predicate, acquire_owner, txn, buf_mgr);

        auto res = executor.Execute();
        if (txn->GetResult() != ResultType::SUCCESS) {
            txn_manager->AbortTransaction(txn);
            return false;
        }
        auto oics = executor.GetResults();
        for (int i = 0; i < oics.size(); ++i) {
            if (oics[i].O_ID > last_o_id) {
                last_o_id = oics[i].O_ID;
            }
        }
    }
    if (last_o_id != -1) {
        {
            Order::OrderKey key{w_id, d_id, last_o_id};

            auto point_lookup = true;
            auto acquire_owner = false;
            auto predicate = [&](const Order &o, bool &should_end_scan) -> bool {
                should_end_scan = true;
                return o.O_W_ID == w_id && o.O_D_ID == d_id && o.O_C_ID == c_id;
            };

            IndexScanExecutor<Order::OrderKey, Order> executor(*orders_table, key, point_lookup, predicate,
                                                               acquire_owner, txn, buf_mgr);
            auto res = executor.Execute();
            if (txn->GetResult() != ResultType::SUCCESS) {
                txn_manager->AbortTransaction(txn);
                return false;
            }
            auto &orders = executor.GetResults();
            assert(orders.size() == 1);
        }

        LOG_TRACE(
                "getOrderLines: SELECT OL_SUPPLY_W_ID, OL_I_ID, OL_QUANTITY, "
                "OL_AMOUNT, OL_DELIVERY_D FROM ORDER_LINE WHERE OL_W_ID = ? AND "
                "OL_D_ID = ? AND OL_O_ID = ?, # w_id, d_id, o_id");

        {
            OrderLine::OrderLineKey ol_key{w_id, d_id, last_o_id, -1};
            bool point_lookup = false;
            bool acquire_onwer = false;
            auto predicate = [&](const OrderLine &ol, bool &should_end_scan) -> bool {
                if (ol.OL_W_ID == w_id && ol.OL_D_ID == d_id && ol.OL_O_ID == last_o_id)
                    return true;
                should_end_scan = true;
                return false;
            };

            IndexScanExecutor<OrderLine::OrderLineKey, OrderLine> executor(*order_line_table, ol_key, point_lookup,
                                                                           predicate, acquire_onwer, txn, buf_mgr);
            auto res = executor.Execute();
            if (txn->GetResult() != ResultType::SUCCESS) {
                txn_manager->AbortTransaction(txn);
                return false;
            }
            auto &order_lines = executor.GetResults();
        }
    }

//    // Construct index scan executor
//    std::vector<oid_t> orders_column_ids = {COL_IDX_O_ID, COL_IDX_O_CARRIER_ID,
//                                            COL_IDX_O_ENTRY_D};
//    std::vector<oid_t> orders_key_column_ids = {COL_IDX_O_W_ID, COL_IDX_O_D_ID,
//                                                COL_IDX_O_C_ID};
//    std::vector<ExpressionType> orders_expr_types;
//    std::vector<type::Value> orders_key_values;
//    std::vector<expression::AbstractExpression *> runtime_keys;
//
//    orders_expr_types.push_back(ExpressionType::COMPARE_EQUAL);
//    orders_key_values.push_back(type::ValueFactory::GetIntegerValue(w_id).Copy());
//    orders_expr_types.push_back(ExpressionType::COMPARE_EQUAL);
//    orders_key_values.push_back(type::ValueFactory::GetIntegerValue(d_id).Copy());
//    orders_expr_types.push_back(ExpressionType::COMPARE_EQUAL);
//    orders_key_values.push_back(type::ValueFactory::GetIntegerValue(c_id).Copy());
//
//    planner::IndexScanPlan::IndexScanDesc orders_index_scan_desc(
//            orders_table_skey_index_oid, orders_key_column_ids, orders_expr_types,
//            orders_key_values, runtime_keys);
//
//    auto predicate = nullptr;
//
//    planner::IndexScanPlan orders_index_scan_node(
//            orders_table, predicate, orders_column_ids, orders_index_scan_desc);
//
//    executor::IndexScanExecutor orders_index_scan_executor(
//            &orders_index_scan_node, context.get());
//
//    // Construct order by executor
//    std::vector<oid_t> sort_keys = {0};
//    std::vector<bool> descend_flags = {true};
//    std::vector<oid_t> output_columns = {0, 1, 2};
//
//    planner::OrderByPlan orders_order_by_node(sort_keys, descend_flags,
//                                              output_columns);
//
//    executor::OrderByExecutor orders_order_by_executor(&orders_order_by_node,
//                                                       context.get());
//    orders_order_by_executor.AddChild(&orders_index_scan_executor);
//
//    // Construct limit executor
//    size_t limit = 1;
//    size_t offset = 0;
//    planner::LimitPlan limit_node(limit, offset);
//    executor::LimitExecutor limit_executor(&limit_node, context.get());
//    limit_executor.AddChild(&orders_order_by_executor);
//
//    auto orders = ExecuteRead(&orders_order_by_executor);
//    if (txn->GetResult() != ResultType::SUCCESS) {
//        txn_manager.AbortTransaction(txn);
//        return false;
//    }
//
//    if (orders.size() != 0) {
//        LOG_TRACE(
//                "getOrderLines: SELECT OL_SUPPLY_W_ID, OL_I_ID, OL_QUANTITY, "
//                "OL_AMOUNT, OL_DELIVERY_D FROM ORDER_LINE WHERE OL_W_ID = ? AND "
//                "OL_D_ID = ? AND OL_O_ID = ?, # w_id, d_id, o_id");
//
//        // Construct index scan executor
//        std::vector<oid_t> order_line_column_ids = {
//                COL_IDX_OL_SUPPLY_W_ID, COL_IDX_OL_I_ID, COL_IDX_OL_QUANTITY,
//                COL_IDX_OL_AMOUNT, COL_IDX_OL_DELIVERY_D};
//        std::vector<oid_t> order_line_key_column_ids = {
//                COL_IDX_OL_W_ID, COL_IDX_OL_D_ID, COL_IDX_OL_O_ID};
//        std::vector<ExpressionType> order_line_expr_types;
//        std::vector<type::Value> order_line_key_values;
//
//        order_line_expr_types.push_back(ExpressionType::COMPARE_EQUAL);
//        order_line_key_values.push_back(
//                type::ValueFactory::GetIntegerValue(w_id).Copy());
//        order_line_expr_types.push_back(ExpressionType::COMPARE_EQUAL);
//        order_line_key_values.push_back(
//                type::ValueFactory::GetIntegerValue(d_id).Copy());
//        order_line_expr_types.push_back(ExpressionType::COMPARE_EQUAL);
//        order_line_key_values.push_back(orders[0][0]);
//
//        planner::IndexScanPlan::IndexScanDesc order_line_index_scan_desc(
//                order_line_table_skey_index_oid, order_line_key_column_ids,
//                order_line_expr_types, order_line_key_values, runtime_keys);
//
//        predicate = nullptr;
//
//        planner::IndexScanPlan order_line_index_scan_node(
//                order_line_table, predicate, order_line_column_ids,
//                order_line_index_scan_desc);
//
//        executor::IndexScanExecutor order_line_index_scan_executor(
//                &order_line_index_scan_node, context.get());
//
//        ExecuteRead(&order_line_index_scan_executor);
//        if (txn->GetResult() != ResultType::SUCCESS) {
//            txn_manager.AbortTransaction(txn);
//            return false;
//        }
//    }

    assert(txn->GetResult() == ResultType::SUCCESS);

    auto result = txn_manager->CommitTransaction(txn);

    if (result == ResultType::SUCCESS) {
        return true;
    } else {
        return false;
    }
}
}
}
}
