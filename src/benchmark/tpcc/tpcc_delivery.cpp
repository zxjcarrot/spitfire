//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tpcc_payment.cpp
//
// Identification: src/main/tpcc/tpcc_delivery.cpp
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
#include <engine/txn.h>
#include <benchmark/tpcc/tpcc_record.h>

#include "util/logger.h"

#include "benchmark/tpcc/tpcc_configuration.h"
#include "benchmark/tpcc/tpcc_loader.h"
#include "benchmark/tpcc/tpcc_workload.h"

#include "engine/executor.h"

namespace spitfire {
namespace benchmark {
namespace tpcc {

bool RunDelivery(const size_t &thread_id, ConcurrentBufferManager *buf_mgr) {
    /*
     "DELIVERY": {
     "getNewOrder": "SELECT NO_O_ID FROM NEW_ORDER WHERE NO_D_ID = ? AND NO_W_ID =
     ? AND NO_O_ID > -1 LIMIT 1", #
     "deleteNewOrder": "DELETE FROM NEW_ORDER WHERE NO_D_ID = ? AND NO_W_ID = ?
     AND NO_O_ID = ?", # d_id, w_id, no_o_id
     "getCId": "SELECT O_C_ID FROM ORDERS WHERE O_ID = ? AND O_D_ID = ? AND O_W_ID
     = ?", # no_o_id, d_id, w_id
     "updateOrders": "UPDATE ORDERS SET O_CARRIER_ID = ? WHERE O_ID = ? AND O_D_ID
     = ? AND O_W_ID = ?", # o_carrier_id, no_o_id, d_id, w_id
     "updateOrderLine": "UPDATE ORDER_LINE SET OL_DELIVERY_D = ? WHERE OL_O_ID = ?
     AND OL_D_ID = ? AND OL_W_ID = ?", # o_entry_d, no_o_id, d_id, w_id
     "sumOLAmount": "SELECT SUM(OL_AMOUNT) FROM ORDER_LINE WHERE OL_O_ID = ? AND
     OL_D_ID = ? AND OL_W_ID = ?", # no_o_id, d_id, w_id
     "updateCustomer": "UPDATE CUSTOMER SET C_BALANCE = C_BALANCE + ? WHERE C_ID =
     ? AND C_D_ID = ? AND C_W_ID = ?", # ol_total, c_id, d_id, w_id
     }
     */

    LOG_TRACE("-------------------------------------");

    /////////////////////////////////////////////////////////
    // PREPARE ARGUMENTS
    /////////////////////////////////////////////////////////
    int warehouse_id = GenerateWarehouseId(thread_id);
    int o_carrier_id =
            GetRandomInteger(orders_min_carrier_id, orders_max_carrier_id);

//    std::vector<expression::AbstractExpression *> runtime_keys;

    /////////////////////////////////////////////////////////
    // BEGIN TRANSACTION
    /////////////////////////////////////////////////////////

    auto txn_manager = MVTOTransactionManager::GetInstance(buf_mgr);

    auto txn = txn_manager->BeginTransaction(thread_id);

//    std::unique_ptr<executor::ExecutorContext> context(
//            new executor::ExecutorContext(txn));

    for (int d_id = 0; d_id < state.districts_per_warehouse; ++d_id) {
        LOG_TRACE(
                "getNewOrder: SELECT NO_O_ID FROM NEW_ORDER WHERE NO_D_ID = ? AND "
                "NO_W_ID = ? AND NO_O_ID > -1 LIMIT 1");

        std::vector<int32_t> new_order_ids;
        {
            bool point_lookup = false;
            bool acquire_owner = false;
            auto start_key = NewOrder::NewOrderKey{d_id,
                                                   warehouse_id,
                                                   0};
            auto predicate = [&](const NewOrder &no, bool &should_end_scan) -> bool {
                if (no.NO_W_ID == warehouse_id && no.NO_D_ID == d_id) {
                    if (no.NO_O_ID > -1) {
                        should_end_scan = true; // Got one, quit scannnig
                        return true;
                    } else {
                        return false; // Keep scanning
                    }
                }
                // Quit scanning
                should_end_scan = true;
                return false;
            };

            IndexScanExecutor<NewOrder::NewOrderKey, NewOrder>
                    new_order_scan_executor(*new_order_table, start_key, point_lookup,
                                            predicate, acquire_owner, txn, buf_mgr);
            auto res = new_order_scan_executor.Execute();
            if (txn->GetResult() != ResultType::SUCCESS) {
                LOG_TRACE("abort transaction");
                txn_manager->AbortTransaction(txn);
                return false;
            }
            auto new_orders = new_order_scan_executor.GetResults();
            for (int i = 0; i < new_orders.size(); ++i) {
                new_order_ids.push_back(new_orders[i].NO_O_ID);
            }
        }

//        // Construct index scan executor
//        std::vector<oid_t> new_order_column_ids = {COL_IDX_NO_O_ID};
//        std::vector<oid_t> new_order_key_column_ids = {
//                COL_IDX_NO_D_ID, COL_IDX_NO_W_ID, COL_IDX_NO_O_ID};
//
//        std::vector<ExpressionType> new_order_expr_types;
//
//        new_order_expr_types.push_back(ExpressionType::COMPARE_EQUAL);
//        new_order_expr_types.push_back(ExpressionType::COMPARE_EQUAL);
//        new_order_expr_types.push_back(ExpressionType::COMPARE_GREATERTHAN);
//
//        std::vector<type::Value> new_order_key_values;
//
//        new_order_key_values.push_back(
//                type::ValueFactory::GetIntegerValue(d_id).Copy());
//        new_order_key_values.push_back(
//                type::ValueFactory::GetIntegerValue(warehouse_id).Copy());
//        new_order_key_values.push_back(
//                type::ValueFactory::GetIntegerValue(-1).Copy());
//
//        planner::IndexScanPlan::IndexScanDesc new_order_idex_scan_desc(
//                new_order_table_pkey_index_oid, new_order_key_column_ids,
//                new_order_expr_types, new_order_key_values, runtime_keys);
//
//        planner::IndexScanPlan new_order_idex_scan_node(new_order_table, nullptr,
//                                                        new_order_column_ids,
//                                                        new_order_idex_scan_desc);
//
//        executor::IndexScanExecutor new_order_index_scan_executor(
//                &new_order_idex_scan_node, context.get());
//
//        // Construct limit executor
//        size_t limit = 1;
//        size_t offset = 0;
//        planner::LimitPlan limit_node(limit, offset);
//        executor::LimitExecutor limit_executor(&limit_node, context.get());
//        limit_executor.AddChild(&new_order_index_scan_executor);
//
//        auto new_order_ids = ExecuteRead(&limit_executor);
//
//        if (txn->GetResult() != ResultType::SUCCESS) {
//            LOG_TRACE("abort transaction");
//            txn_manager.AbortTransaction(txn);
//            return false;
//        }

        if (new_order_ids.size() == 0) {
            // TODO:  No orders for this district: skip it. Note: This must be
            // reported if > 1%
            continue;
        }

        assert(new_order_ids.size() == 1);

        // result: NO_O_ID
        auto no_o_id = new_order_ids[0];

        LOG_TRACE("no_o_id = %d", type::ValuePeeker::PeekInteger(no_o_id));

        LOG_TRACE(
                "getCId: SELECT O_C_ID FROM ORDERS WHERE O_ID = ? AND O_D_ID = ? AND "
                "O_W_ID = ?");
        std::vector<int32_t> order_c_ids;
        {
            bool point_lookup = true;
            bool acquire_owner = false;
            auto key = Order::OrderKey{warehouse_id,
                                       d_id,
                                       no_o_id};
            auto predicate = [&](const Order &o, bool &should_end_scan) -> bool {
                should_end_scan = true;
                return o.O_W_ID == warehouse_id && o.O_D_ID == d_id && o.O_ID == no_o_id;
            };
            IndexScanExecutor<Order::OrderKey, Order> executor(*orders_table, key, point_lookup,
                                                               predicate, acquire_owner, txn, buf_mgr);
            auto res = executor.Execute();
            if (txn->GetResult() != ResultType::SUCCESS) {
                LOG_TRACE("abort transaction");
                txn_manager->AbortTransaction(txn);
                return false;
            }
            auto orders = executor.GetResults();
            for (int i = 0; i < orders.size(); ++i) {
                order_c_ids.push_back(orders[i].O_C_ID);
            }
        }
//        std::vector<oid_t> orders_column_ids = {COL_IDX_O_C_ID};
//        std::vector<oid_t> orders_key_column_ids = {COL_IDX_O_ID, COL_IDX_O_D_ID,
//                                                    COL_IDX_O_W_ID};
//
//        std::vector<ExpressionType> orders_expr_types;
//
//        orders_expr_types.push_back(ExpressionType::COMPARE_EQUAL);
//        orders_expr_types.push_back(ExpressionType::COMPARE_EQUAL);
//        orders_expr_types.push_back(ExpressionType::COMPARE_EQUAL);
//
//        std::vector<type::Value> orders_key_values;
//
//        orders_key_values.push_back(no_o_id);
//        orders_key_values.push_back(
//                type::ValueFactory::GetIntegerValue(d_id).Copy());
//        orders_key_values.push_back(
//                type::ValueFactory::GetIntegerValue(warehouse_id).Copy());
//
//        planner::IndexScanPlan::IndexScanDesc orders_index_scan_desc(
//                orders_table_pkey_index_oid, orders_key_column_ids, orders_expr_types,
//                orders_key_values, runtime_keys);
//
//        // Create the index scan plan node
//        planner::IndexScanPlan orders_index_scan_node(
//                orders_table, nullptr, orders_column_ids, orders_index_scan_desc);
//
//        // Create the executors
//        executor::IndexScanExecutor orders_index_scan_executor(
//                &orders_index_scan_node, context.get());
//
//        auto orders_ids = ExecuteRead(&orders_index_scan_executor);
//
//        if (txn->GetResult() != ResultType::SUCCESS) {
//            LOG_TRACE("abort transaction");
//            txn_manager.AbortTransaction(txn);
//            return false;
//        }

        assert(order_c_ids.size() == 1);

        // Result: O_C_ID
        auto c_id = order_c_ids[0];

        LOG_TRACE(
                "sumOLAmount: SELECT SUM(OL_AMOUNT) FROM ORDER_LINE WHERE OL_O_ID = ? "
                "AND OL_D_ID = ? AND OL_W_ID = ?");

        double ol_total = 0;
        {
            bool point_lookup = false;
            bool acquire_owner = false;
            auto start_key = OrderLine::OrderLineKey{warehouse_id,
                                                     d_id,
                                                     no_o_id,
                                                     -1};
            auto predicate = [&](const OrderLine &ol, bool &should_end_scan) -> bool {
                if (ol.OL_W_ID == warehouse_id && ol.OL_D_ID == d_id && ol.OL_O_ID == no_o_id) {
                    return true;
                }
                should_end_scan = true;
                return false;
            };
            IndexScanExecutor<OrderLine::OrderLineKey, OrderLine> executor(*order_line_table, start_key, point_lookup,
                                                                           predicate, acquire_owner, txn, buf_mgr);
            auto res = executor.Execute();
            if (txn->GetResult() != ResultType::SUCCESS) {
                LOG_TRACE("abort transaction");
                txn_manager->AbortTransaction(txn);
                return false;
            }
            auto order_lines = executor.GetResults();
            for (int i = 0; i < order_lines.size(); ++i) {
                ol_total += order_lines[i].OL_AMOUNT;
            }
        }

//        // Construct index scan executor
//        std::vector<oid_t> order_line_column_ids = {COL_IDX_OL_AMOUNT};
//        std::vector<oid_t> order_line_key_column_ids = {
//                COL_IDX_OL_O_ID, COL_IDX_OL_D_ID, COL_IDX_OL_W_ID};
//
//        std::vector<ExpressionType> order_line_expr_types;
//
//        order_line_expr_types.push_back(ExpressionType::COMPARE_EQUAL);
//        order_line_expr_types.push_back(ExpressionType::COMPARE_EQUAL);
//        order_line_expr_types.push_back(ExpressionType::COMPARE_EQUAL);
//
//        std::vector<type::Value> order_line_key_values;
//
//        order_line_key_values.push_back(no_o_id);
//        order_line_key_values.push_back(
//                type::ValueFactory::GetIntegerValue(d_id).Copy());
//        order_line_key_values.push_back(
//                type::ValueFactory::GetIntegerValue(warehouse_id).Copy());
//
//        planner::IndexScanPlan::IndexScanDesc order_line_index_scan_desc(
//                order_line_table_pkey_index_oid, order_line_key_column_ids,
//                order_line_expr_types, order_line_key_values, runtime_keys);
//
//        planner::IndexScanPlan order_line_index_scan_node(
//                order_line_table, nullptr, order_line_column_ids,
//                order_line_index_scan_desc);
//
//        executor::IndexScanExecutor order_line_index_scan_executor(
//                &order_line_index_scan_node, context.get());
//
//        auto order_line_index_scan_res =
//                ExecuteRead(&order_line_index_scan_executor);
//
//        if (txn->GetResult() != ResultType::SUCCESS) {
//            LOG_TRACE("abort transaction");
//            txn_manager.AbortTransaction(txn);
//            return false;
//        }
//
//        double sum_res = 0.0;
//
//        // Workaround: Externanl sum
//        for (auto v : order_line_index_scan_res) {
//            assert(v.size() == 1);
//            sum_res += type::ValuePeeker::PeekDouble(v[0]);
//        }

        //auto ol_total = type::ValueFactory::GetDecimalValue(sum_res);

        LOG_TRACE(
                "deleteNewOrder: DELETE FROM NEW_ORDER WHERE NO_D_ID = ? AND NO_W_ID = "
                "? AND NO_O_ID = ?");


        {
            auto key = NewOrder::NewOrderKey{d_id,
                                             warehouse_id,
                                             no_o_id};
            auto predicate = [&](const NewOrder &no) -> bool {
                return no.NO_O_ID == no_o_id && no.NO_D_ID == d_id && no.NO_W_ID == warehouse_id;
            };
            PointDeleteExecutor<NewOrder::NewOrderKey, NewOrder> executor(*new_order_table, key,
                                                                          predicate, txn, buf_mgr);
            auto res = executor.Execute();
            if (txn->GetResult() != ResultType::SUCCESS) {
                LOG_TRACE("abort transaction");
                txn_manager->AbortTransaction(txn);
                return false;
            }
        }

//        // Construct index scan executor
//        std::vector<oid_t> new_order_delete_column_ids = {0};
//
//        std::vector<ExpressionType> new_order_delete_expr_types;
//
//        new_order_delete_expr_types.push_back(ExpressionType::COMPARE_EQUAL);
//        new_order_delete_expr_types.push_back(ExpressionType::COMPARE_EQUAL);
//        new_order_delete_expr_types.push_back(ExpressionType::COMPARE_EQUAL);
//
//        std::vector<type::Value> new_order_delete_key_values;
//
//        new_order_delete_key_values.push_back(
//                type::ValueFactory::GetIntegerValue(d_id).Copy());
//        new_order_delete_key_values.push_back(
//                type::ValueFactory::GetIntegerValue(warehouse_id).Copy());
//        new_order_delete_key_values.push_back(no_o_id);
//
//        planner::IndexScanPlan::IndexScanDesc new_order_delete_idex_scan_desc(
//                new_order_table_pkey_index_oid, new_order_key_column_ids,
//                new_order_delete_expr_types, new_order_delete_key_values, runtime_keys);
//
//        // Create index scan plan node
//        planner::IndexScanPlan new_order_delete_idex_scan_node(
//                new_order_table, nullptr, new_order_delete_column_ids,
//                new_order_delete_idex_scan_desc);
//
//        // Create executors
//        executor::IndexScanExecutor new_order_delete_index_scan_executor(
//                &new_order_delete_idex_scan_node, context.get());
//
//        // Construct delete executor
//        planner::DeletePlan new_order_delete_node(new_order_table);
//
//        executor::DeleteExecutor new_order_delete_executor(&new_order_delete_node,
//                                                           context.get());
//
//        new_order_delete_executor.AddChild(&new_order_delete_index_scan_executor);
//
//        // Execute the query
//        ExecuteDelete(&new_order_delete_executor);
//
//        // Check if aborted
//        if (txn->GetResult() != ResultType::SUCCESS) {
//            LOG_TRACE("abort transaction");
//            txn_manager.AbortTransaction(txn);
//            return false;
//        }

        LOG_TRACE(
                "updateOrders: UPDATE ORDERS SET O_CARRIER_ID = ? WHERE O_ID = ? AND "
                "O_D_ID = ? AND O_W_ID = ?");


        {
            auto key = Order::OrderKey{warehouse_id,
                                       d_id,
                                       no_o_id};
            auto predicate = [&](const Order &o) -> bool {
                return o.O_W_ID == warehouse_id && o.O_D_ID == d_id && o.O_ID == no_o_id;
            };
            auto updater = [&](Order &o) {
                o.O_CARRIER_ID = o_carrier_id;
            };
            PointUpdateExecutor<Order::OrderKey, Order> executor(*orders_table, key,
                                                                 predicate, updater, txn, buf_mgr);
            auto res = executor.Execute();
            if (txn->GetResult() != ResultType::SUCCESS) {
                LOG_TRACE("abort transaction");
                txn_manager->AbortTransaction(txn);
                return false;
            }
        }
//        // Construct index scan executor
//        std::vector<oid_t> orders_update_column_ids = {COL_IDX_O_CARRIER_ID};
//
//        std::vector<type::Value> orders_update_key_values;
//
//        orders_update_key_values.push_back(no_o_id);
//        orders_update_key_values.push_back(
//                type::ValueFactory::GetIntegerValue(d_id).Copy());
//        orders_update_key_values.push_back(
//                type::ValueFactory::GetIntegerValue(warehouse_id).Copy());
//
//        planner::IndexScanPlan::IndexScanDesc orders_update_index_scan_desc(
//                orders_table_pkey_index_oid, orders_key_column_ids, orders_expr_types,
//                orders_update_key_values, runtime_keys);
//
//        // Reuse the index scan desc created above since nothing different
//        planner::IndexScanPlan orders_update_index_scan_node(
//                orders_table, nullptr, orders_update_column_ids,
//                orders_update_index_scan_desc);
//
//        executor::IndexScanExecutor orders_update_index_scan_executor(
//                &orders_update_index_scan_node, context.get());
//
//        // Construct update executor
//        TargetList orders_target_list;
//        DirectMapList orders_direct_map_list;
//
//        size_t orders_column_count = 8;
//        for (oid_t col_itr = 0; col_itr < orders_column_count; col_itr++) {
//            // Skip O_CARRIER_ID
//            if (col_itr != COL_IDX_O_CARRIER_ID) {
//                orders_direct_map_list.emplace_back(col_itr,
//                                                    std::make_pair(0, col_itr));
//            }
//        }
//        type::Value orders_update_val =
//                type::ValueFactory::GetIntegerValue(o_carrier_id).Copy();
//
//        planner::DerivedAttribute {
//                expression::ExpressionUtil::ConstantValueFactory(orders_update_val)};
//        orders_target_list.emplace_back(COL_IDX_O_CARRIER_ID, carrier_id);
//
//        std::unique_ptr<const planner::ProjectInfo> orders_project_info(
//                new planner::ProjectInfo(std::move(orders_target_list),
//                                         std::move(orders_direct_map_list)));
//        planner::UpdatePlan orders_update_node(orders_table,
//                                               std::move(orders_project_info));
//
//        executor::UpdateExecutor orders_update_executor(&orders_update_node,
//                                                        context.get());
//
//        orders_update_executor.AddChild(&orders_update_index_scan_executor);
//
//        // Execute the query
//        ExecuteUpdate(&orders_update_executor);
//
//        if (txn->GetResult() != ResultType::SUCCESS) {
//            LOG_TRACE("abort transaction");
//            txn_manager.AbortTransaction(txn);
//            return false;
//        }

        LOG_TRACE(
                "updateOrderLine: UPDATE ORDER_LINE SET OL_DELIVERY_D = ? WHERE "
                "OL_O_ID = ? AND OL_D_ID = ? AND OL_W_ID = ?");


        {
            auto start_key = OrderLine::OrderLineKey{warehouse_id,
                                                     d_id,
                                                     no_o_id,
                                                     -1};
            auto predicate = [&](const OrderLine &ol) -> bool {
                return ol.OL_W_ID == warehouse_id && ol.OL_D_ID == d_id && ol.OL_O_ID == no_o_id;
            };

            auto updater = [&](OrderLine &ol) {
                ol.OL_DELIVERY_D = 0;
            };
            ScanUpdateExecutor<OrderLine::OrderLineKey, OrderLine> executor(*order_line_table, start_key, predicate,
                                                                            updater, txn, buf_mgr);
            auto res = executor.Execute();
            if (txn->GetResult() != ResultType::SUCCESS) {
                LOG_TRACE("abort transaction");
                txn_manager->AbortTransaction(txn);
                return false;
            }
        }
//
//        // Construct index scan executor
//        std::vector<oid_t> order_line_update_column_ids = {COL_IDX_OL_DELIVERY_D};
//
//        std::vector<type::Value> order_line_update_key_values;
//
//        order_line_update_key_values.push_back(no_o_id);
//        order_line_update_key_values.push_back(
//                type::ValueFactory::GetIntegerValue(d_id).Copy());
//        order_line_update_key_values.push_back(
//                type::ValueFactory::GetIntegerValue(warehouse_id).Copy());
//
//        planner::IndexScanPlan::IndexScanDesc order_line_update_index_scan_desc(
//                order_line_table_pkey_index_oid, order_line_key_column_ids,
//                order_line_expr_types, order_line_update_key_values, runtime_keys);
//
//        planner::IndexScanPlan order_line_update_index_scan_node(
//                order_line_table, nullptr, order_line_update_column_ids,
//                order_line_update_index_scan_desc);
//
//        executor::IndexScanExecutor order_line_update_index_scan_executor(
//                &order_line_update_index_scan_node, context.get());
//
//        // Construct update executor
//        TargetList order_line_target_list;
//        DirectMapList order_line_direct_map_list;
//
//        size_t order_line_column_count = 10;
//        for (oid_t col_itr = 0; col_itr < order_line_column_count; col_itr++) {
//            // Skip OL_DELIVERY_D
//            if (col_itr != COL_IDX_OL_DELIVERY_D) {
//                order_line_direct_map_list.emplace_back(col_itr,
//                                                        std::make_pair(0, col_itr));
//            }
//        }
//        type::Value order_line_update_val =
//                type::ValueFactory::GetTimestampValue(0).Copy();
//
//        planner::DerivedAttribute delivery_id{
//                expression::ExpressionUtil::ConstantValueFactory(
//                        order_line_update_val)};
//        order_line_target_list.emplace_back(COL_IDX_OL_DELIVERY_D, delivery_id);
//
//        std::unique_ptr<const planner::ProjectInfo> order_line_project_info(
//                new planner::ProjectInfo(std::move(order_line_target_list),
//                                         std::move(order_line_direct_map_list)));
//        planner::UpdatePlan order_line_update_node(
//                order_line_table, std::move(order_line_project_info));
//
//        executor::UpdateExecutor order_line_update_executor(&order_line_update_node,
//                                                            context.get());
//
//        order_line_update_executor.AddChild(&order_line_update_index_scan_executor);
//
//        ExecuteUpdate(&order_line_update_executor);
//
//        if (txn->GetResult() != ResultType::SUCCESS) {
//            LOG_TRACE("abort transaction");
//            txn_manager.AbortTransaction(txn);
//            return false;
//        }

        LOG_TRACE(
                "updateCustomer: UPDATE CUSTOMER SET C_BALANCE = C_BALANCE + ? WHERE "
                "C_ID = ? AND C_D_ID = ? AND C_W_ID = ?");
        {
            auto key = Customer::CustomerKey{warehouse_id,
                                             d_id,
                                             c_id};
            auto predicate = [&](const Customer &c) -> bool {
                return c.C_ID == c_id && c.C_D_ID == d_id && c.C_W_ID == warehouse_id;
            };

            auto updater = [&](Customer &c) {
                c.C_BALANCE += ol_total;
            };
            PointUpdateExecutor<Customer::CustomerKey, Customer> executor(*customer_table, key, predicate, updater, txn,
                                                                          buf_mgr);
            auto res = executor.Execute();
            if (txn->GetResult() != ResultType::SUCCESS) {
                LOG_TRACE("abort transaction");
                txn_manager->AbortTransaction(txn);
                return false;
            }
        }
//        // Construct index scan executor
//        std::vector<oid_t> customer_column_ids = {COL_IDX_C_BALANCE};
//        std::vector<oid_t> customer_key_column_ids = {COL_IDX_C_ID, COL_IDX_C_D_ID,
//                                                      COL_IDX_C_W_ID};
//
//        std::vector<ExpressionType> customer_expr_types;
//
//        customer_expr_types.push_back(ExpressionType::COMPARE_EQUAL);
//        customer_expr_types.push_back(ExpressionType::COMPARE_EQUAL);
//        customer_expr_types.push_back(ExpressionType::COMPARE_EQUAL);
//
//        std::vector<type::Value> customer_key_values;
//
//        customer_key_values.push_back(c_id);
//        customer_key_values.push_back(
//                type::ValueFactory::GetIntegerValue(d_id).Copy());
//        customer_key_values.push_back(
//                type::ValueFactory::GetIntegerValue(warehouse_id).Copy());
//
//        planner::IndexScanPlan::IndexScanDesc customer_index_scan_desc(
//                customer_table_pkey_index_oid, customer_key_column_ids,
//                customer_expr_types, customer_key_values, runtime_keys);
//
//        planner::IndexScanPlan customer_index_scan_node(
//                customer_table, nullptr, customer_column_ids, customer_index_scan_desc);
//
//        executor::IndexScanExecutor customer_index_scan_executor(
//                &customer_index_scan_node, context.get());
//
//        // Construct update executor
//        TargetList customer_target_list;
//        DirectMapList customer_direct_map_list;
//
//        size_t customer_column_count = 21;
//        for (oid_t col_itr = 0; col_itr < customer_column_count; col_itr++) {
//            // Skip OL_DELIVERY_D
//            if (col_itr != COL_IDX_C_BALANCE) {
//                customer_direct_map_list.emplace_back(col_itr,
//                                                      std::make_pair(0, col_itr));
//            }
//        }
//
//        // Expressions
//        // Tuple value expression
//        auto tuple_val_expr = expression::ExpressionUtil::TupleValueFactory(
//                type::TypeId::INTEGER, 0, COL_IDX_C_BALANCE);
//        // Constant value expression
//        auto constant_val_expr =
//                expression::ExpressionUtil::ConstantValueFactory(ol_total);
//        // + operator expression
//        auto plus_operator_expr = expression::ExpressionUtil::OperatorFactory(
//                ExpressionType::OPERATOR_PLUS, type::TypeId::INTEGER, tuple_val_expr,
//                constant_val_expr);
//
//        planner::DerivedAttribute c_balance{plus_operator_expr};
//        customer_target_list.emplace_back(COL_IDX_C_BALANCE, c_balance);
//
//        std::unique_ptr<const planner::ProjectInfo> customer_project_info(
//                new planner::ProjectInfo(std::move(customer_target_list),
//                                         std::move(customer_direct_map_list)));
//        planner::UpdatePlan customer_update_node(customer_table,
//                                                 std::move(customer_project_info));
//
//        executor::UpdateExecutor customer_update_executor(&customer_update_node,
//                                                          context.get());
//
//        customer_update_executor.AddChild(&customer_index_scan_executor);
//
//        // Execute the query
//        ExecuteUpdate(&customer_update_executor);
//
//        if (txn->GetResult() != ResultType::SUCCESS) {
//            LOG_TRACE("abort transaction");
//            txn_manager.AbortTransaction(txn);
//            return false;
//        }
    }

    assert(txn->GetResult() == ResultType::SUCCESS);

    auto result = txn_manager->CommitTransaction(txn);

    if (result == ResultType::SUCCESS) {
        LOG_TRACE("commit successfully");
        return true;
    } else {
        assert(result == ResultType::ABORTED || result == ResultType::FAILURE);
        return false;
    }
}
}
}
}
