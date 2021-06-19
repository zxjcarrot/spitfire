//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tpcc_stock_level.cpp
//
// Identification: src/main/tpcc/tpcc_stock_level.cpp
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
#include <unordered_set>
#include <vector>
#include <benchmark/tpcc/tpcc_record.h>

#include "benchmark/tpcc/tpcc_configuration.h"
#include "benchmark/tpcc/tpcc_loader.h"
#include "benchmark/tpcc/tpcc_workload.h"

#include "engine/executor.h"
#include "util/logger.h"

namespace spitfire {
namespace benchmark {
namespace tpcc {

bool RunStockLevel(const size_t &thread_id, ConcurrentBufferManager *buf_mgr) {
    /*
       "STOCK_LEVEL": {
       "getOId": "SELECT D_NEXT_O_ID FROM DISTRICT WHERE D_W_ID = ? AND D_ID = ?",
       "getStockCount": "SELECT COUNT(DISTINCT(OL_I_ID)) FROM ORDER_LINE, STOCK
       WHERE OL_W_ID = ? AND OL_D_ID = ? AND OL_O_ID < ? AND OL_O_ID >= ? AND
       S_W_ID = ? AND S_I_ID = OL_I_ID AND S_QUANTITY < ?
       }
     */
    auto txn_manager = MVTOTransactionManager::GetInstance(buf_mgr);
    auto txn = txn_manager->BeginTransaction(thread_id);

//    std::unique_ptr<executor::ExecutorContext> context(
//            new executor::ExecutorContext(txn));

    // Prepare random data
    int w_id = GenerateWarehouseId(thread_id);
    int d_id = GetRandomInteger(0, state.districts_per_warehouse - 1);
    int threshold = GetRandomInteger(stock_min_threshold, stock_max_threshold);

    int32_t o_id;
    LOG_TRACE(
            "getOId: SELECT D_NEXT_O_ID FROM DISTRICT WHERE D_W_ID = ? AND D_ID = ?");
    {
        District::DistrictKey key{w_id, d_id};
        bool point_lookup = true;
        bool acquire_owner = false;
        auto predicate = [&](const District &d, bool &should_end_scan) -> bool {
            should_end_scan = true;
            return d.D_W_ID == w_id && d.D_ID == d_id;
        };
        IndexScanExecutor<District::DistrictKey, District> executor(*district_table, key, point_lookup, predicate,
                                                                    acquire_owner, txn, buf_mgr);
        auto res = executor.Execute();
        if (txn->GetResult() != ResultType::SUCCESS) {
            txn_manager->AbortTransaction(txn);
            return false;
        }
        assert(executor.GetResults().size() == 1);
        o_id == executor.GetResults()[0].D_NEXT_O_ID;
    }
    // Construct index scan executor
//    std::vector<oid_t> district_column_ids = {COL_IDX_D_NEXT_O_ID};
//    std::vector<oid_t> district_key_column_ids = {COL_IDX_D_W_ID, COL_IDX_D_ID};
//    std::vector<ExpressionType> district_expr_types;
//    std::vector<type::Value> district_key_values;
//    std::vector<expression::AbstractExpression *> runtime_keys;
//
//    district_expr_types.push_back(ExpressionType::COMPARE_EQUAL);
//    district_key_values.push_back(
//            type::ValueFactory::GetIntegerValue(w_id).Copy());
//    district_expr_types.push_back(ExpressionType::COMPARE_EQUAL);
//    district_key_values.push_back(
//            type::ValueFactory::GetIntegerValue(d_id).Copy());
//
//    planner::IndexScanPlan::IndexScanDesc district_index_scan_desc(
//            district_table_pkey_index_oid, district_key_column_ids,
//            district_expr_types, district_key_values, runtime_keys);
//
//    expression::AbstractExpression *predicate = nullptr;
//    planner::IndexScanPlan district_index_scan_node(
//            district_table, predicate, district_column_ids, district_index_scan_desc);
//    executor::IndexScanExecutor district_index_scan_executor(
//            &district_index_scan_node, context.get());
//
//    auto districts = ExecuteRead(&district_index_scan_executor);
//    if (txn->GetResult() != ResultType::SUCCESS) {
//        txn_manager.AbortTransaction(txn);
//        return false;
//    }
//    if (districts.size() != 1) {
//        LOG_ERROR("incorrect districts size : %lu", districts.size());
//        assert(false);
//    }

    //type::Value o_id = districts[0][0];

    LOG_TRACE(
            "getStockCount: SELECT COUNT(DISTINCT(OL_I_ID)) FROM ORDER_LINE, STOCK  "
            "WHERE OL_W_ID = ? AND OL_D_ID = ? AND OL_O_ID < ? AND OL_O_ID >= ? AND "
            "S_W_ID = ? AND S_I_ID = OL_I_ID AND S_QUANTITY < ?");

    //int max_o_id = type::ValuePeeker::PeekInteger(o_id);
    //int min_o_id = max_o_id - 20;
    int max_o_id = o_id;
    int min_o_id = max_o_id - 20;

    std::unordered_set<int> distinct_items;
    for (int curr_o_id = min_o_id; curr_o_id < max_o_id; ++curr_o_id) {
        OrderLine::OrderLineKey start_key{w_id, d_id, curr_o_id, -1};

        bool point_lookup = false;
        bool acquire_owner = false;
        auto predicate = [&](const OrderLine &ol, bool &should_end_scan) -> bool {
            if (ol.OL_O_ID == curr_o_id && ol.OL_W_ID == w_id && ol.OL_D_ID == d_id)
                return true;
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
        auto &order_lines = executor.GetResults();
        if (order_lines.size() == 0) {
            LOG_TRACE("order line return size incorrect : %lu",
                      order_lines.size());
            continue;
        }

        // Index Join
        for (int j = 0; j < order_lines.size(); ++j) {
            int32_t item_id = order_lines[j].OL_I_ID;
            Stock::StockKey key{w_id, item_id};
            bool point_lookup = true;
            bool acquire_owner = false;
            auto predicate = [w_id, item_id](const Stock &s, bool &should_end_scan) -> bool {
                should_end_scan = true;
                return s.S_W_ID == w_id && s.S_I_ID == item_id;
            };
            IndexScanExecutor<Stock::StockKey, Stock> stock_scan_executor(*stock_table, key, point_lookup, predicate,
                                                                          acquire_owner, txn, buf_mgr);
            auto res = stock_scan_executor.Execute();
            if (txn->GetResult() != ResultType::SUCCESS) {
                LOG_TRACE("abort transaction");
                txn_manager->AbortTransaction(txn);
                return false;
            }

            assert(stock_scan_executor.GetResults().size() == 1);
            auto &stock = stock_scan_executor.GetResults()[0];
            if (stock.S_QUANTITY < threshold) {
                distinct_items.insert(stock.S_I_ID);
            }
        }
    }

//    //////////////////////////////////////////////////////////////
//    std::vector<oid_t> order_line_column_ids = {COL_IDX_OL_I_ID};
//    std::vector<oid_t> order_line_key_column_ids = {
//            COL_IDX_OL_W_ID, COL_IDX_OL_D_ID, COL_IDX_OL_O_ID};
//    std::vector<ExpressionType> order_line_expr_types;
//    order_line_expr_types.push_back(ExpressionType::COMPARE_EQUAL);
//    order_line_expr_types.push_back(ExpressionType::COMPARE_EQUAL);
//    order_line_expr_types.push_back(ExpressionType::COMPARE_EQUAL);
//
//    auto order_line_skey_index =
//            order_line_table->GetIndexWithOid(order_line_table_skey_index_oid);
//
//    //////////////////////////////////////////////////////////////
//    std::vector<oid_t> stock_column_ids = {COL_IDX_S_QUANTITY};
//    std::vector<oid_t> stock_key_column_ids = {COL_IDX_S_W_ID, COL_IDX_S_I_ID};
//    std::vector<ExpressionType> stock_expr_types;
//    stock_expr_types.push_back(ExpressionType::COMPARE_EQUAL);
//    stock_expr_types.push_back(ExpressionType::COMPARE_EQUAL);
//
//    auto stock_pkey_index =
//            stock_table->GetIndexWithOid(stock_table_pkey_index_oid);
//
//    //////////////////////////////////////////////////////////////
//    std::unordered_set<int> distinct_items;
//
//    for (int curr_o_id = min_o_id; curr_o_id < max_o_id; ++curr_o_id) {
//        ////////////////////////////////////////////////////////////////
//        /////////// Construct left table index scan ////////////////////
//        ////////////////////////////////////////////////////////////////
//
//        std::vector<type::Value> order_line_key_values;
//
//        order_line_key_values.push_back(
//                type::ValueFactory::GetIntegerValue(w_id).Copy());
//        order_line_key_values.push_back(
//                type::ValueFactory::GetIntegerValue(d_id).Copy());
//        order_line_key_values.push_back(
//                type::ValueFactory::GetIntegerValue(curr_o_id).Copy());
//
//        planner::IndexScanPlan::IndexScanDesc order_line_index_scan_desc(
//                order_line_table_skey_index_oid, order_line_key_column_ids,
//                order_line_expr_types, order_line_key_values, runtime_keys);
//
//        planner::IndexScanPlan order_line_index_scan_node(
//                order_line_table, nullptr, order_line_column_ids,
//                order_line_index_scan_desc);
//
//        executor::IndexScanExecutor order_line_index_scan_executor(
//                &order_line_index_scan_node, context.get());
//
//        auto order_line_values = ExecuteRead(&order_line_index_scan_executor);
//
//        if (txn->GetResult() != ResultType::SUCCESS) {
//            LOG_TRACE("abort transaction");
//            txn_manager.AbortTransaction(txn);
//            return false;
//        }
//
//        if (order_line_values.size() == 0) {
//            LOG_TRACE("order line return size incorrect : %lu",
//                      order_line_values.size());
//            continue;
//        }
//
//        auto item_id = order_line_values[0][0];
//
//        LOG_TRACE("item_id: %s", item_id.GetInfo().c_str());
//
//        //////////////////////////////////////////////////////////////////
//        ///////////// Construct right table index scan ///////////////////
//        //////////////////////////////////////////////////////////////////
//
//        std::vector<type::Value> stock_key_values;
//
//        stock_key_values.push_back(
//                type::ValueFactory::GetIntegerValue(w_id).Copy());
//        stock_key_values.push_back(item_id);
//
//        planner::IndexScanPlan::IndexScanDesc stock_index_scan_desc(
//                stock_table_pkey_index_oid, stock_key_column_ids, stock_expr_types,
//                stock_key_values, runtime_keys);
//
//        // Add predicate S_QUANTITY < threshold
//        planner::IndexScanPlan stock_index_scan_node(
//                stock_table, nullptr, stock_column_ids, stock_index_scan_desc);
//
//        executor::IndexScanExecutor stock_index_scan_executor(
//                &stock_index_scan_node, context.get());
//
//        auto stock_values = ExecuteRead(&stock_index_scan_executor);
//
//        if (txn->GetResult() != ResultType::SUCCESS) {
//            LOG_TRACE("abort transaction");
//            txn_manager.AbortTransaction(txn);
//            return false;
//        }
//
//        if (stock_values.size() == 0) {
//            // LOG_ERROR("stock return size incorrect : %lu",
//            // order_line_values.size());
//            continue;
//        }
//
//        auto quantity = stock_values[0][0];
//        if (type::ValuePeeker::PeekInteger(quantity) < threshold) {
//            distinct_items.insert(type::ValuePeeker::PeekInteger(item_id));
//        }
//    }
    LOG_TRACE("number of distinct items=%lu", distinct_items.size());

    assert(txn->GetResult() == ResultType::SUCCESS);

    auto result = txn_manager->CommitTransaction(txn);

    if (result == ResultType::SUCCESS) {
        return true;
    } else {
        return false;
    }

    return true;
}
}
}
}
