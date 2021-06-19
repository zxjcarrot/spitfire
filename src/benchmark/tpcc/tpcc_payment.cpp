//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tpcc_payment.cpp
//
// Identification: src/main/tpcc/tpcc_payment.cpp
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

#include "benchmark/tpcc/tpcc_configuration.h"
#include "benchmark/tpcc/tpcc_loader.h"
#include "benchmark/tpcc/tpcc_workload.h"

#include "engine/executor.h"
#include "util/logger.h"

namespace spitfire {
namespace benchmark {
namespace tpcc {

bool RunPayment(const size_t &thread_id, ConcurrentBufferManager *buf_mgr) {
    /*
       "PAYMENT": {
       "getWarehouse": "SELECT W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE,
       W_ZIP FROM WAREHOUSE WHERE W_ID = ?", # w_id
       "updateWarehouseBalance": "UPDATE WAREHOUSE SET W_YTD = W_YTD + ? WHERE
       W_ID = ?", # h_amount, w_id
       "getDistrict": "SELECT D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE,
       D_ZIP FROM DISTRICT WHERE D_W_ID = ? AND D_ID = ?", # w_id, d_id
       "updateDistrictBalance": "UPDATE DISTRICT SET D_YTD = D_YTD + ? WHERE
       D_W_ID = ? AND D_ID = ?", # h_amount, d_w_id, d_id
       "getCustomerByCustomerId": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST,
       C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT,
       C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DATA
       FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?", # w_id, d_id,
       c_id
       "getCustomersByLastName": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST,
       C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT,
       C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DATA
       FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_LAST = ? ORDER BY
       C_FIRST", # w_id, d_id, c_last
       "updateBCCustomer": "UPDATE CUSTOMER SET C_BALANCE = ?, C_YTD_PAYMENT = ?,
       C_PAYMENT_CNT = ?, C_DATA = ? WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID =
       ?", # c_balance, c_ytd_payment, c_payment_cnt, c_data, c_w_id, c_d_id, c_id
       "updateGCCustomer": "UPDATE CUSTOMER SET C_BALANCE = ?, C_YTD_PAYMENT = ?,
       C_PAYMENT_CNT = ? WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?", #
       c_balance, c_ytd_payment, c_payment_cnt, c_w_id, c_d_id, c_id
       "insertHistory": "INSERT INTO HISTORY VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
       }
     */

    LOG_TRACE("-------------------------------------");

    /////////////////////////////////////////////////////////
    // PREPARE ARGUMENTS
    /////////////////////////////////////////////////////////
    int warehouse_id = GenerateWarehouseId(thread_id);
    int district_id = GetRandomInteger(0, state.districts_per_warehouse - 1);
    int customer_warehouse_id;
    int customer_district_id;
    int customer_id = -1;
    std::string customer_lastname;
    double h_amount =
            GetRandomFixedPoint(2, payment_min_amount, payment_max_amount);
    // WARN: Hard code the date as 0. may cause problem
    int h_date = 0;

    int x = GetRandomInteger(1, 100);
    // currently we only retrieve data by id.
    int y = 100;  // GetRandomInteger(1, 100);

    // 85%: paying through own warehouse ( or there is only 1 warehouse)
    if (state.warehouse_count == 1 || x <= 85) {
        customer_warehouse_id = warehouse_id;
        customer_district_id = district_id;
    }
        // 15%: paying through another warehouse
    else {
        customer_warehouse_id =
                GetRandomIntegerExcluding(0, state.warehouse_count - 1, warehouse_id);
        assert(customer_warehouse_id != warehouse_id);
        customer_district_id =
                GetRandomInteger(0, state.districts_per_warehouse - 1);
    }

    // 60%: payment by last name
    if (y <= 60) {
        LOG_TRACE("By last name");
        customer_lastname = GetRandomLastName(state.customers_per_district);
    }
        // 40%: payment by id
    else {
        LOG_TRACE("By id");
        customer_id = GetRandomInteger(0, state.customers_per_district - 1);
    }

    //std::vector<expression::AbstractExpression *> runtime_keys;

    /////////////////////////////////////////////////////////
    // BEGIN TRANSACTION
    /////////////////////////////////////////////////////////

    auto txn_manager = MVTOTransactionManager::GetInstance(buf_mgr);

    auto txn = txn_manager->BeginTransaction(thread_id);

    Customer customer;
    if (customer_id >= 0) {
        LOG_TRACE(
                "getCustomerByCustomerId:  WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = "
                "? , # w_id = %d, d_id = %d, c_id = %d",
                warehouse_id, district_id, customer_id);
        Customer::CustomerKey customer_key{warehouse_id, district_id, customer_id};
        bool point_lookup = true;
        bool acquire_owner = false;
        auto predicate = [&](const Customer &c, bool &should_end_scan) -> bool {
            should_end_scan = true;
            return c.C_W_ID == warehouse_id && c.C_D_ID == district_id && c.C_ID == customer_id;
        };

        IndexScanExecutor<Customer::CustomerKey, Customer> executor(*customer_table, customer_key, point_lookup,
                                                                    predicate, acquire_owner, txn, buf_mgr);
        auto res = executor.Execute();

        // Check if aborted
        if (txn->GetResult() != ResultType::SUCCESS) {
            LOG_TRACE("abort transaction");
            txn_manager->AbortTransaction(txn);
            return false;
        }

        auto &customers = executor.GetResults();
        if (customers.size() != 1) {
            assert(false);
        }
        customer = customers[0];
    } else {
        CustomerIndexedColumns cic;
        cic.C_W_ID = warehouse_id;
        cic.C_D_ID = district_id;
        cic.C_ID = -1;
        strcpy(cic.C_LAST, customer_lastname.c_str());
        bool point_lookup = false;
        bool acquire_owner = false;
        auto predicate = [&](const CustomerIndexedColumns &cic, bool &should_end_scan) -> bool {
            if (cic.C_W_ID == warehouse_id && cic.C_D_ID == district_id &&
                std::string(cic.C_LAST) == customer_lastname) {
                return true;
            }
            should_end_scan = true;
            return false;
        };
        IndexScanExecutor<CustomerIndexedColumns, CustomerIndexedColumns> cic_scan_executor(*customer_skey_table, cic,
                                                                                            point_lookup, predicate,
                                                                                            acquire_owner, txn,
                                                                                            buf_mgr);

        auto res = cic_scan_executor.Execute();
        // Check if aborted
        if (txn->GetResult() != ResultType::SUCCESS) {
            LOG_TRACE("abort transaction");
            txn_manager->AbortTransaction(txn);
            return false;
        }

        auto cics = cic_scan_executor.GetResults();
        assert(cics.size() > 0);
        std::vector<Customer> customers;
        for (int i = 0; i < cics.size(); ++i) {
            int32_t c_id = cics[i].C_ID;
            auto customer_key = Customer::CustomerKey{warehouse_id, district_id, c_id};
            bool point_lookup = true;
            bool acquire_owner = false;
            auto predicate = [&](const Customer &c, bool &should_end_scan) -> bool {
                should_end_scan = true;
                return cic.C_W_ID == warehouse_id && cic.C_D_ID == district_id && c.C_ID == c_id;
            };
            IndexScanExecutor<Customer::CustomerKey, Customer> executor(*customer_table, customer_key, point_lookup,
                                                                        predicate, acquire_owner, txn, buf_mgr);

            auto res = executor.Execute();
            // Check if aborted
            if (txn->GetResult() != ResultType::SUCCESS) {
                LOG_TRACE("abort transaction");
                txn_manager->AbortTransaction(txn);
                return false;
            }
            assert(executor.GetResults().size() == 1);
            customers.push_back(executor.GetResults()[0]);
        }

        // Hack: sort customers by C_FIRST in-memory
        std::sort(customers.begin(), customers.end(), [&](const Customer &lhs, const Customer &rhs) {
            return std::string(lhs.C_FIRST) < std::string(rhs.C_FIRST);
        });
        // Get the midpoint customer
        customer = customers[(customers.size() - 1) / 2];
    }
//    std::unique_ptr<executor::ExecutorContext> context(
//            new executor::ExecutorContext(txn));
//
//    std::vector<type::Value> customer;
//
//    if (customer_id >= 0) {
//        LOG_TRACE(
//                "getCustomerByCustomerId:  WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = "
//                "? , # w_id = %d, d_id = %d, c_id = %d",
//                warehouse_id, district_id, customer_id);
//
//        std::vector<oid_t> customer_column_ids = {
//                0, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 20};
//
//        std::vector<oid_t> customer_pkey_column_ids = {0, 1, 2};
//        std::vector<ExpressionType> customer_pexpr_types;
//        customer_pexpr_types.push_back(ExpressionType::COMPARE_EQUAL);
//        customer_pexpr_types.push_back(ExpressionType::COMPARE_EQUAL);
//        customer_pexpr_types.push_back(ExpressionType::COMPARE_EQUAL);
//
//        std::vector<type::Value> customer_pkey_values;
//
//        customer_pkey_values.push_back(
//                type::ValueFactory::GetIntegerValue(customer_id).Copy());
//        customer_pkey_values.push_back(
//                type::ValueFactory::GetIntegerValue(district_id).Copy());
//        customer_pkey_values.push_back(
//                type::ValueFactory::GetIntegerValue(warehouse_id).Copy());
//
//        planner::IndexScanPlan::IndexScanDesc customer_pindex_scan_desc(
//                customer_table_pkey_index_oid, customer_pkey_column_ids,
//                customer_pexpr_types, customer_pkey_values, runtime_keys);
//
//        planner::IndexScanPlan customer_pindex_scan_node(customer_table, nullptr,
//                                                         customer_column_ids,
//                                                         customer_pindex_scan_desc);
//
//        executor::IndexScanExecutor customer_pindex_scan_executor(
//                &customer_pindex_scan_node, context.get());
//
//        auto customer_list = ExecuteRead(&customer_pindex_scan_executor);
//
//        // Check if aborted
//        if (txn->GetResult() != ResultType::SUCCESS) {
//            LOG_TRACE("abort transaction");
//            txn_manager.AbortTransaction(txn);
//            return false;
//        }
//
//        if (customer_list.size() != 1) {
//            assert(false);
//        }
//
//        customer = customer_list[0];
//
//    } else {
//        assert(customer_lastname.empty() == false);
//
//        LOG_TRACE(
//                "getCustomersByLastName: WHERE C_W_ID = ? AND C_D_ID = ? AND C_LAST = "
//                "? ORDER BY C_FIRST, # w_id = %d, d_id = %d, c_last = %s",
//                warehouse_id, district_id, customer_lastname.c_str());
//
//        std::vector<oid_t> customer_column_ids = {
//                0, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 20};
//
//        std::vector<oid_t> customer_key_column_ids = {1, 2, 5};
//        std::vector<ExpressionType> customer_expr_types;
//        customer_expr_types.push_back(ExpressionType::COMPARE_EQUAL);
//        customer_expr_types.push_back(ExpressionType::COMPARE_EQUAL);
//        customer_expr_types.push_back(ExpressionType::COMPARE_EQUAL);
//
//        std::vector<type::Value> customer_key_values;
//
//        customer_key_values.push_back(
//                type::ValueFactory::GetIntegerValue(district_id).Copy());
//        customer_key_values.push_back(
//                type::ValueFactory::GetIntegerValue(warehouse_id).Copy());
//        customer_key_values.push_back(
//                type::ValueFactory::GetVarcharValue(customer_lastname).Copy());
//
//        planner::IndexScanPlan::IndexScanDesc customer_index_scan_desc(
//                customer_table_skey_index_oid, customer_key_column_ids,
//                customer_expr_types, customer_key_values, runtime_keys);
//
//        planner::IndexScanPlan customer_index_scan_node(
//                customer_table, nullptr, customer_column_ids, customer_index_scan_desc);
//
//        executor::IndexScanExecutor customer_index_scan_executor(
//                &customer_index_scan_node, context.get());
//
//        auto customer_list = ExecuteRead(&customer_index_scan_executor);
//
//        // Check if aborted
//        if (txn->GetResult() != ResultType::SUCCESS) {
//            LOG_TRACE("abort transaction");
//            txn_manager.AbortTransaction(txn);
//            return false;
//        }
//
//        if (customer_list.size() < 1) {
//            LOG_INFO("C_W_ID=%d, C_D_ID=%d", warehouse_id, district_id);
//            assert(false);
//        }
//
//        // Get the midpoint customer's id
//        auto mid_pos = (customer_list.size() - 1) / 2;
//        customer = customer_list[mid_pos];
//    }

    LOG_TRACE("getWarehouse:WHERE W_ID = ? # w_id = %d", warehouse_id);

    Warehouse warehouse;
    {
        int vv = 0;
        int32_t warehouse_key = warehouse_id;
        bool point_lookup = true;
        bool acquire_onwer = false;
        auto predicate = [&](const Warehouse &w, bool &should_end_scan) -> bool {
            should_end_scan = true;
            auto v = w.W_ID == warehouse_id;
            assert(v);
//            if (!v)
//                ++vv;
            return v;
        };

        IndexScanExecutor<int32_t, Warehouse> executor(*warehouse_table, warehouse_key, point_lookup, predicate,
                                                       acquire_onwer, txn, buf_mgr);
        auto res = executor.Execute();
        // Check if aborted
        if (txn->GetResult() != ResultType::SUCCESS) {
            LOG_TRACE("abort transaction");
            txn_manager->AbortTransaction(txn);
            return false;
        }
//        if (executor.GetResults().size() != 1) {
//            IndexScanExecutor<int32_t, Warehouse> executor(*warehouse_table, warehouse_key, point_lookup, predicate,
//                                                           acquire_onwer, txn, buf_mgr);
//            auto res = executor.Execute();
//            // Check if aborted
//            if (txn->GetResult() != ResultType::SUCCESS) {
//                LOG_TRACE("abort transaction");
//                txn_manager->AbortTransaction(txn);
//                return false;
//            }
//            assert(executor.GetResults().size() == 1);
//        }
        assert(executor.GetResults().size() == 1);
        warehouse = executor.GetResults()[0];
    }
//    std::vector<oid_t> warehouse_key_column_ids = {0};
//    std::vector<ExpressionType> warehouse_expr_types;
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
//    std::vector<oid_t> warehouse_column_ids = {1, 2, 3, 4, 5, 6, 8};
//
//    planner::IndexScanPlan warehouse_index_scan_node(warehouse_table, nullptr,
//                                                     warehouse_column_ids,
//                                                     warehouse_index_scan_desc);
//
//    executor::IndexScanExecutor warehouse_index_scan_executor(
//            &warehouse_index_scan_node, context.get());
//
//    // Execute the query
//    auto warehouse_list = ExecuteRead(&warehouse_index_scan_executor);
//
//    // Check if aborted
//    if (txn->GetResult() != ResultType::SUCCESS) {
//        LOG_TRACE("abort transaction");
//        txn_manager.AbortTransaction(txn);
//        return false;
//    }
//
//    if (warehouse_list.size() != 1) {
//        assert(false);
//    }

    LOG_TRACE(
            "getDistrict: WHERE D_W_ID = ? AND D_ID = ?, # w_id = %d, d_id = %d",
            warehouse_id, district_id);
    // We also retrieve the original D_YTD from this query,
    // which is not the standard TPCC approach

    District district;
    {
        District::DistrictKey district_key{warehouse_id, district_id};

        bool point_lookup = true;
        bool acquire_onwer = false;
        auto predicate = [&](const District &d, bool &should_end_scan) -> bool {
            should_end_scan = true;
            return d.D_W_ID == warehouse_id && d.D_ID == district_id;
        };

        IndexScanExecutor<District::DistrictKey, District> executor(*district_table, district_key, point_lookup,
                                                                    predicate, acquire_onwer, txn, buf_mgr);
        auto res = executor.Execute();
        // Check if aborted
        if (txn->GetResult() != ResultType::SUCCESS) {
            LOG_TRACE("abort transaction");
            txn_manager->AbortTransaction(txn);
            return false;
        }
        assert(executor.GetResults().size() == 1);
        district = executor.GetResults()[0];
    }

//    std::vector<oid_t> district_key_column_ids = {0, 1};
//    std::vector<ExpressionType> district_expr_types;
//    district_expr_types.push_back(ExpressionType::COMPARE_EQUAL);
//    district_expr_types.push_back(ExpressionType::COMPARE_EQUAL);
//
//    std::vector<type::Value> district_key_values;
//
//    district_key_values.push_back(
//            type::ValueFactory::GetIntegerValue(district_id).Copy());
//    district_key_values.push_back(
//            type::ValueFactory::GetIntegerValue(warehouse_id).Copy());
//
//    planner::IndexScanPlan::IndexScanDesc district_index_scan_desc(
//            district_table_pkey_index_oid, district_key_column_ids,
//            district_expr_types, district_key_values, runtime_keys);
//
//    std::vector<oid_t> district_column_ids = {2, 3, 4, 5, 6, 7, 9};
//
//    planner::IndexScanPlan district_index_scan_node(
//            district_table, nullptr, district_column_ids, district_index_scan_desc);
//
//    executor::IndexScanExecutor district_index_scan_executor(
//            &district_index_scan_node, context.get());
//
//    // Execute the query
//    auto district_list = ExecuteRead(&district_index_scan_executor);
//
//    // Check if aborted
//    if (txn->GetResult() != ResultType::SUCCESS) {
//        LOG_TRACE("abort transaction");
//        txn_manager.AbortTransaction(txn);
//        return false;
//    }
//
//    if (district_list.size() != 1) {
//        assert(false);
//    }

//    double warehouse_new_balance =
//            type::ValuePeeker::PeekDouble(warehouse_list[0][6]) + h_amount;

    double warehouse_new_balance = warehouse.W_YTD + h_amount;

    LOG_TRACE(
            "updateWarehouseBalance: UPDATE WAREHOUSE SET W_YTD = W_YTD + ? WHERE "
            "W_ID = ?,# h_amount = %f, w_id = %d",
            h_amount, warehouse_id);

    {
        auto warehouse_key = warehouse_id;
        auto predicate = [&](const Warehouse &w) -> bool {
            return w.W_ID == warehouse_id;
        };
        auto updater = [&](Warehouse &w) {
            w.W_YTD = warehouse_new_balance;
        };

        PointUpdateExecutor<int32_t, Warehouse> executor(*warehouse_table, warehouse_key, predicate, updater, txn,
                                                         buf_mgr);
        auto res = executor.Execute();
        // Check if aborted
        if (txn->GetResult() != ResultType::SUCCESS) {
            LOG_TRACE("abort transaction");
            txn_manager->AbortTransaction(txn);
            return false;
        }
    }
//    std::vector<oid_t> warehouse_update_column_ids = {8};
//
//    std::vector<type::Value> warehouse_update_key_values;
//
//    warehouse_update_key_values.push_back(
//            type::ValueFactory::GetIntegerValue(warehouse_id).Copy());
//
//    planner::IndexScanPlan::IndexScanDesc warehouse_update_index_scan_desc(
//            warehouse_table_pkey_index_oid, warehouse_key_column_ids,
//            warehouse_expr_types, warehouse_update_key_values, runtime_keys);
//
//    planner::IndexScanPlan warehouse_update_index_scan_node(
//            warehouse_table, nullptr, warehouse_update_column_ids,
//            warehouse_update_index_scan_desc);
//
//    executor::IndexScanExecutor warehouse_update_index_scan_executor(
//            &warehouse_update_index_scan_node, context.get());
//
//    TargetList warehouse_target_list;
//    DirectMapList warehouse_direct_map_list;
//
//    // Keep the first 8 columns unchanged
//    for (oid_t col_itr = 0; col_itr < 8; ++col_itr) {
//        warehouse_direct_map_list.emplace_back(col_itr,
//                                               std::pair<oid_t, oid_t>(0, col_itr));
//    }
//    // Update the 9th column
//    type::Value warehouse_new_balance_value =
//            type::ValueFactory::GetDecimalValue(warehouse_new_balance).Copy();
//
//    planner::DerivedAttribute warehouse_bal{
//            expression::ExpressionUtil::ConstantValueFactory(
//                    warehouse_new_balance_value)};
//    warehouse_target_list.emplace_back(8, warehouse_bal);
//
//    std::unique_ptr<const planner::ProjectInfo> warehouse_project_info(
//            new planner::ProjectInfo(std::move(warehouse_target_list),
//                                     std::move(warehouse_direct_map_list)));
//    planner::UpdatePlan warehouse_update_node(warehouse_table,
//                                              std::move(warehouse_project_info));
//
//    executor::UpdateExecutor warehouse_update_executor(&warehouse_update_node,
//                                                       context.get());
//
//    warehouse_update_executor.AddChild(&warehouse_update_index_scan_executor);
//
//    // Execute the query
//    ExecuteUpdate(&warehouse_update_executor);
//
//    // Check if aborted
//    if (txn->GetResult() != ResultType::SUCCESS) {
//        LOG_TRACE("abort transaction");
//        txn_manager.AbortTransaction(txn);
//        return false;
//    }


//    double district_new_balance =
//            type::ValuePeeker::PeekDouble(district_list[0][6]) + h_amount;
    double district_new_balance = district.D_YTD + h_amount;

    LOG_TRACE(
            "updateDistrictBalance: UPDATE DISTRICT SET D_YTD = D_YTD + ? WHERE "
            "D_W_ID = ? AND D_ID = ?,# h_amount = %f, d_w_id = %d, d_id = %d",
            h_amount, district_id, warehouse_id);

    {
        auto district_key = District::DistrictKey{warehouse_id, district_id};
        auto predicate = [&](const District &d) -> bool {
            return d.D_W_ID == warehouse_id && d.D_ID == district_id;
        };
        auto updater = [&](District &d) {
            d.D_YTD = district_new_balance;
        };

        PointUpdateExecutor<District::DistrictKey, District> executor(*district_table, district_key, predicate, updater,
                                                                      txn, buf_mgr);
        auto res = executor.Execute();
        // Check if aborted
        if (txn->GetResult() != ResultType::SUCCESS) {
            LOG_TRACE("abort transaction");
            txn_manager->AbortTransaction(txn);
            return false;
        }
    }
//    std::vector<oid_t> district_update_column_ids = {9};
//
//    std::vector<type::Value> district_update_key_values;
//
//    district_update_key_values.push_back(
//            type::ValueFactory::GetIntegerValue(district_id).Copy());
//    district_update_key_values.push_back(
//            type::ValueFactory::GetIntegerValue(warehouse_id).Copy());
//
//    planner::IndexScanPlan::IndexScanDesc district_update_index_scan_desc(
//            district_table_pkey_index_oid, district_key_column_ids,
//            district_expr_types, district_update_key_values, runtime_keys);
//
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
//    // Keep all columns unchanged except for the
//    for (oid_t col_itr = 0; col_itr < 11; ++col_itr) {
//        if (col_itr != 9) {
//            district_direct_map_list.emplace_back(
//                    col_itr, std::pair<oid_t, oid_t>(0, col_itr));
//        }
//    }
//    // Update the 10th column
//    type::Value district_new_balance_value =
//            type::ValueFactory::GetDecimalValue(district_new_balance).Copy();
//
//    planner::DerivedAttribute district_bal{
//            expression::ExpressionUtil::ConstantValueFactory(
//                    district_new_balance_value)};
//    district_target_list.emplace_back(9, district_bal);
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
//    // Execute the query
//    ExecuteUpdate(&district_update_executor);
//
//    // Check the result
//    if (txn->GetResult() != ResultType::SUCCESS) {
//        LOG_TRACE("abort transaction");
//        txn_manager.AbortTransaction(txn);
//        return false;
//    }

    //std::string customer_credit = type::ValuePeeker::PeekVarchar(customer[11]);
    std::string customer_credit(customer.C_CREDIT, sizeof(customer.C_CREDIT));
    //double customer_balance =
    //        type::ValuePeeker::PeekDouble(customer[14]) - h_amount;
    double customer_balance = customer.C_BALANCE - h_amount;
    //double customer_ytd_payment =
    //        type::ValuePeeker::PeekDouble(customer[15]) + h_amount;
    double customer_ytd_payment = customer.C_YTD_PAYMENT + h_amount;
    //int customer_payment_cnt = type::ValuePeeker::PeekInteger(customer[16]) + 1;
    int customer_payment_cnt = customer.C_PAYMENT_CNT + 1;

    //customer_id = type::ValuePeeker::PeekInteger(customer[0]);
    customer_id = customer.C_ID;

    // NOTE: Workaround, we assign a constant to the customer's data field

    // Check the credit record of the user
    if (customer_credit == customers_bad_credit) {
        LOG_TRACE(
                "updateBCCustomer:# c_balance = %f, c_ytd_payment = %f, c_payment_cnt "
                "= %d, c_data = %s, c_w_id = %d, c_d_id = %d, c_id = %d",
                customer_balance, customer_ytd_payment, customer_payment_cnt,
                data_constant.c_str(), customer_warehouse_id, customer_district_id,
                customer_id);

        auto predicate = [&](const Customer &c) -> bool {
            return c.C_W_ID == customer_warehouse_id && c.C_D_ID == customer_district_id && c.C_ID == customer_id;
        };
        auto updater = [&](Customer &c) {
            c.C_BALANCE = customer_balance;
            c.C_YTD_PAYMENT = customer_ytd_payment;
            c.C_PAYMENT_CNT = customer_payment_cnt;
            memcpy(c.C_DATA, data_constant.c_str(), sizeof(c.C_DATA));
        };

        Customer::CustomerKey key{customer_warehouse_id, customer_district_id, customer_id};

        PointUpdateExecutor<Customer::CustomerKey, Customer> executor(*customer_table, key, predicate, updater, txn,
                                                                      buf_mgr);
        auto res = executor.Execute();
        // Check if aborted
        if (txn->GetResult() != ResultType::SUCCESS) {
            LOG_TRACE("abort transaction");
            txn_manager->AbortTransaction(txn);
            return false;
        }
//        std::vector<oid_t> customer_pkey_column_ids = {0, 1, 2};
//        std::vector<ExpressionType> customer_pexpr_types;
//        customer_pexpr_types.push_back(ExpressionType::COMPARE_EQUAL);
//        customer_pexpr_types.push_back(ExpressionType::COMPARE_EQUAL);
//        customer_pexpr_types.push_back(ExpressionType::COMPARE_EQUAL);
//
//        std::vector<type::Value> customer_pkey_values;
//
//        customer_pkey_values.push_back(
//                type::ValueFactory::GetIntegerValue(customer_id).Copy());
//        customer_pkey_values.push_back(
//                type::ValueFactory::GetIntegerValue(customer_district_id).Copy());
//        customer_pkey_values.push_back(
//                type::ValueFactory::GetIntegerValue(customer_warehouse_id).Copy());
//
//        planner::IndexScanPlan::IndexScanDesc customer_pindex_scan_desc(
//                customer_table_pkey_index_oid, customer_pkey_column_ids,
//                customer_pexpr_types, customer_pkey_values, runtime_keys);
//
//        std::vector<oid_t> customer_update_bc_column_ids = {16, 17, 18, 20};
//
//        // Create update executor
//        planner::IndexScanPlan customer_update_bc_index_scan_node(
//                customer_table, nullptr, customer_update_bc_column_ids,
//                customer_pindex_scan_desc);
//
//        executor::IndexScanExecutor customer_update_bc_index_scan_executor(
//                &customer_update_bc_index_scan_node, context.get());
//
//        TargetList customer_bc_target_list;
//        DirectMapList customer_bc_direct_map_list;
//
//        // Only update the 17th to 19th and the 21th columns
//        for (oid_t col_itr = 0; col_itr < 21; ++col_itr) {
//            if ((col_itr >= 16 && col_itr <= 18) || (col_itr == 20)) {
//                continue;
//            }
//            customer_bc_direct_map_list.emplace_back(
//                    col_itr, std::pair<oid_t, oid_t>(0, col_itr));
//        }
//
//        type::Value customer_new_balance_value =
//                type::ValueFactory::GetDecimalValue(customer_balance).Copy();
//        type::Value customer_new_ytd_value =
//                type::ValueFactory::GetDecimalValue(customer_ytd_payment).Copy();
//        type::Value customer_new_paycnt_value =
//                type::ValueFactory::GetIntegerValue(customer_payment_cnt).Copy();
//        type::Value customer_new_data_value =
//                type::ValueFactory::GetVarcharValue(data_constant.c_str()).Copy();
//
//        planner::DerivedAttribute c_new_bal{
//                expression::ExpressionUtil::ConstantValueFactory(
//                        customer_new_balance_value)};
//        planner::DerivedAttribute c_new_ytd{
//                expression::ExpressionUtil::ConstantValueFactory(
//                        customer_new_ytd_value)};
//        planner::DerivedAttribute c_new_paycnt{
//                expression::ExpressionUtil::ConstantValueFactory(
//                        customer_new_paycnt_value)};
//        planner::DerivedAttribute c_new_data{
//                expression::ExpressionUtil::ConstantValueFactory(
//                        customer_new_data_value)};
//
//        customer_bc_target_list.emplace_back(16, c_new_bal);
//        customer_bc_target_list.emplace_back(17, c_new_ytd);
//        customer_bc_target_list.emplace_back(18, c_new_paycnt);
//        customer_bc_target_list.emplace_back(20, c_new_data);
//
//        std::unique_ptr<const planner::ProjectInfo> customer_bc_project_info(
//                new planner::ProjectInfo(std::move(customer_bc_target_list),
//                                         std::move(customer_bc_direct_map_list)));
//
//        planner::UpdatePlan customer_update_bc_node(
//                customer_table, std::move(customer_bc_project_info));
//
//        executor::UpdateExecutor customer_update_bc_executor(
//                &customer_update_bc_node, context.get());
//
//        customer_update_bc_executor.AddChild(
//                &customer_update_bc_index_scan_executor);
//
//        // Execute the query
//        ExecuteUpdate(&customer_update_bc_executor);
    } else {
        LOG_TRACE(
                "updateGCCustomer: # c_balance = %f, c_ytd_payment = %f, c_payment_cnt "
                "= %d, c_w_id = %d, c_d_id = %d, c_id = %d",
                customer_balance, customer_ytd_payment, customer_payment_cnt,
                customer_warehouse_id, customer_district_id, customer_id);

        auto predicate = [&](const Customer &c) -> bool {
            return c.C_W_ID == customer_warehouse_id && c.C_D_ID == customer_district_id && c.C_ID == customer_id;
        };
        auto updater = [&](Customer &c) {
            c.C_BALANCE = customer_balance;
            c.C_YTD_PAYMENT = customer_ytd_payment;
            c.C_PAYMENT_CNT = customer_payment_cnt;
        };

        Customer::CustomerKey key{customer_warehouse_id, customer_district_id, customer_id};

        PointUpdateExecutor<Customer::CustomerKey, Customer> executor(*customer_table, key, predicate, updater, txn,
                                                                      buf_mgr);
        auto res = executor.Execute();
        // Check if aborted
        if (txn->GetResult() != ResultType::SUCCESS) {
            LOG_TRACE("abort transaction");
            txn_manager->AbortTransaction(txn);
            return false;
        }
//
//        std::vector<oid_t> customer_pkey_column_ids = {0, 1, 2};
//        std::vector<ExpressionType> customer_pexpr_types;
//        customer_pexpr_types.push_back(ExpressionType::COMPARE_EQUAL);
//        customer_pexpr_types.push_back(ExpressionType::COMPARE_EQUAL);
//        customer_pexpr_types.push_back(ExpressionType::COMPARE_EQUAL);
//
//        std::vector<type::Value> customer_pkey_values;
//
//        customer_pkey_values.push_back(
//                type::ValueFactory::GetIntegerValue(customer_id).Copy());
//        customer_pkey_values.push_back(
//                type::ValueFactory::GetIntegerValue(customer_district_id).Copy());
//        customer_pkey_values.push_back(
//                type::ValueFactory::GetIntegerValue(customer_warehouse_id).Copy());
//
//        planner::IndexScanPlan::IndexScanDesc customer_pindex_scan_desc(
//                customer_table_pkey_index_oid, customer_pkey_column_ids,
//                customer_pexpr_types, customer_pkey_values, runtime_keys);
//
//        std::vector<oid_t> customer_update_gc_column_ids = {16, 17, 18};
//
//        // Create update executor
//        planner::IndexScanPlan customer_update_gc_index_scan_node(
//                customer_table, nullptr, customer_update_gc_column_ids,
//                customer_pindex_scan_desc);
//
//        executor::IndexScanExecutor customer_update_gc_index_scan_executor(
//                &customer_update_gc_index_scan_node, context.get());
//
//        TargetList customer_gc_target_list;
//        DirectMapList customer_gc_direct_map_list;
//
//        // Only update the 17th to 19th columns
//        for (oid_t col_itr = 0; col_itr < 21; ++col_itr) {
//            if (col_itr >= 16 && col_itr <= 18) {
//                continue;
//            }
//            customer_gc_direct_map_list.emplace_back(
//                    col_itr, std::pair<oid_t, oid_t>(0, col_itr));
//        }
//        type::Value customer_new_balance_value =
//                type::ValueFactory::GetDecimalValue(customer_balance).Copy();
//        type::Value customer_new_ytd_value =
//                type::ValueFactory::GetDecimalValue(customer_ytd_payment).Copy();
//        type::Value customer_new_paycnt_value =
//                type::ValueFactory::GetIntegerValue(customer_payment_cnt).Copy();
//
//        planner::DerivedAttribute c_new_bal{
//                expression::ExpressionUtil::ConstantValueFactory(
//                        customer_new_balance_value)};
//        planner::DerivedAttribute c_new_ytd{
//                expression::ExpressionUtil::ConstantValueFactory(
//                        customer_new_ytd_value)};
//        planner::DerivedAttribute c_new_paycnt{
//                expression::ExpressionUtil::ConstantValueFactory(
//                        customer_new_paycnt_value)};
//
//        customer_gc_target_list.emplace_back(16, c_new_bal);
//        customer_gc_target_list.emplace_back(17, c_new_ytd);
//        customer_gc_target_list.emplace_back(18, c_new_paycnt);
//
//        std::unique_ptr<const planner::ProjectInfo> customer_gc_project_info(
//                new planner::ProjectInfo(std::move(customer_gc_target_list),
//                                         std::move(customer_gc_direct_map_list)));
//
//        planner::UpdatePlan customer_update_gc_node(
//                customer_table, std::move(customer_gc_project_info));
//
//        executor::UpdateExecutor customer_update_gc_executor(
//                &customer_update_gc_node, context.get());
//
//        customer_update_gc_executor.AddChild(
//                &customer_update_gc_index_scan_executor);
//
//        // Execute the query
//        ExecuteUpdate(&customer_update_gc_executor);
    }

//    // Check the result
//    if (txn->GetResult() != ResultType::SUCCESS) {
//        LOG_TRACE("abort transaction");
//        txn_manager.AbortTransaction(txn);
//        return false;
//    }

    LOG_TRACE(
            "insertHistory: INSERT INTO HISTORY VALUES (?, ?, ?, ?, ?, ?, ?, ?)");

    History history_tuple = BuildHistoryTuple(customer_id, customer_district_id, customer_warehouse_id, district_id,
                                              warehouse_id);
    history_tuple.H_DATE = h_date;
    history_tuple.H_AMOUNT = h_amount;
    memcpy(history_tuple.H_DATA, data_constant.data(), std::min(sizeof(history_tuple.H_DATA), data_constant.size()));
    InsertExecutor<int32_t, History> history_insert_executor(*history_table, history_tuple, txn, buf_mgr);
    auto res = history_insert_executor.Execute();
//    std::unique_ptr<storage::Tuple> history_tuple(
//            new storage::Tuple(history_table->GetSchema(), true));
//
//    // H_C_ID
//    history_tuple->SetValue(0, type::ValueFactory::GetIntegerValue(customer_id),
//                            nullptr);
//    // H_C_D_ID
//    history_tuple->SetValue(
//            1, type::ValueFactory::GetIntegerValue(customer_district_id), nullptr);
//    // H_C_W_ID
//    history_tuple->SetValue(
//            2, type::ValueFactory::GetIntegerValue(customer_warehouse_id), nullptr);
//    // H_D_ID
//    history_tuple->SetValue(3, type::ValueFactory::GetIntegerValue(district_id),
//                            nullptr);
//    // H_W_ID
//    history_tuple->SetValue(4, type::ValueFactory::GetIntegerValue(warehouse_id),
//                            nullptr);
//    // H_DATE
//    history_tuple->SetValue(5, type::ValueFactory::GetTimestampValue(h_date),
//                            nullptr);
//    // H_AMOUNT
//    history_tuple->SetValue(6, type::ValueFactory::GetDecimalValue(h_amount),
//                            nullptr);
//    // H_DATA
//    // Note: workaround
//    history_tuple->SetValue(7, type::ValueFactory::GetVarcharValue(data_constant),
//                            context.get()->GetPool());
//
//    planner::InsertPlan history_insert_node(history_table,
//                                            std::move(history_tuple));
//    executor::InsertExecutor history_insert_executor(&history_insert_node,
//                                                     context.get());
//
//    // Execute
//    history_insert_executor.Execute();
//
//    // Check result
    if (txn->GetResult() != ResultType::SUCCESS) {
        LOG_TRACE("abort transaction");
        txn_manager->AbortTransaction(txn);
        return false;
    }

    assert(txn->GetResult() == ResultType::SUCCESS);

    auto result = txn_manager->CommitTransaction(txn);

    if (result == ResultType::SUCCESS) {
        return true;
    } else {
        assert(result == ResultType::ABORTED ||
               result == ResultType::FAILURE);
        return false;
    }
}
}
}
}
