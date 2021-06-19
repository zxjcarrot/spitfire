//
// Created by zxjcarrot on 2020-03-22.
//

#ifndef SPITFIRE_SIMULATED_ANNEALING_H
#define SPITFIRE_SIMULATED_ANNEALING_H

#include "buf/buf_mgr.h"

namespace spitfire {
namespace benchmark {

void SimulatedAnnealing(int duration_in_seconds,PageMigrationPolicy init_config,
                        std::function<void(const PageMigrationPolicy &new_config)> set_config,
                        std::function<double()> get_current_config_cost,
                        std::function<PageMigrationPolicy(const PageMigrationPolicy &)> get_neighboring_config);

void GradientDescent(int duration_in_seconds, PageMigrationPolicy init_config,
                     std::function<void(const PageMigrationPolicy &new_config)> set_config,
                     std::function<double()> get_current_config_cost);
}
}
#endif //SPITFIRE_SIMULATED_ANNEALING_H
