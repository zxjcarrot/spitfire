//
// Created by zxjcarrot on 2020-03-22.
//

#include "buf/buf_mgr.h"
#include "benchmark/minimizer.h"
#include "benchmark/ycsb/ycsb_configuration.h"
#include "util/logger.h"

namespace spitfire {
namespace benchmark {

void SimulatedAnnealing(int duration_in_seconds, PageMigrationPolicy init_config,
                        std::function<void(const PageMigrationPolicy &new_config)> set_config,
                        std::function<double()> get_current_config_cost,
                        std::function<PageMigrationPolicy(const PageMigrationPolicy &)> get_neighboring_config) {
    std::random_device rd;
    std::mt19937 generator(rd());
    std::uniform_real_distribution<> distribution(0.0, 1.0);
    std::uniform_real_distribution<> distribution_01(0.0, 1.0);
    // cooling parameter
    double alpha = 0.9;
    const double e = 2.718281828;
    double temperature = 800;
    double temperature_min = 0.00008;
    int state_count = 15;
    int state_itr = 0;

    // initial state
    auto current_configuration = init_config;
    auto new_configuration = init_config;
    double energy = 0;
    double new_energy = 0;
    double acceptance_probablility = 0;

    int operation_itr = 0;
    int print_period = 5; // 10s per print
    int tuning_period = print_period; // 10s per tuning
    int init_period = print_period;
    int operation_duration = duration_in_seconds - init_period;
    int tuning_rounds = 0;
    set_config(init_config);
    while (operation_itr < operation_duration) {
        operation_itr++;

        // Wait some time before initialization - Warmup
        if (operation_itr < init_period) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }

        // Initialization
        if (operation_itr == init_period) {
            energy = get_current_config_cost();
            LOG_INFO("Initial Configuration = %s\t, and E(x)= %f", current_configuration.ToString().c_str(), energy);
            continue;
        }
//
//        // Print current energy
//        if (operation_itr % print_period == 0) {
//            LOG_INFO("Current Configuration = %s\t, and E(x)= %f", current_configuration.ToString().c_str(), energy);
//        }

        if (operation_itr % tuning_period == 0) {
            ++tuning_rounds;
            new_energy = get_current_config_cost();
            double energy_delta = (new_energy - energy);

            if (energy_delta < 0) {
                acceptance_probablility = 1.0;
            } else {
                acceptance_probablility = pow(e, -(energy_delta) / temperature);
                LOG_INFO("DELTA: %f\t AP: %f", energy_delta, acceptance_probablility);
            }

            // Switch to new configuration
            if (energy_delta < 0 || (distribution_01(generator)) <= acceptance_probablility) {
                // Accept new policy configuration
                current_configuration = new_configuration;
                state_itr++;
                if (energy_delta < 0){
                    LOG_INFO("NEW (ENERGY)  :: %f :: STATE ITR: %d", acceptance_probablility, state_itr);
                } else {
                    LOG_INFO("NEW (TRIAL)  :: %f :: STATE ITR: %d", acceptance_probablility, state_itr);
                }
                // Update energy
                energy = new_energy;
            }
                // Switch back to current configuration
            else {
                LOG_INFO("OLD           :: %f", acceptance_probablility);
                set_config(current_configuration);
            }

            LOG_INFO("Current Configuration = %s\t, and E(x)= %f, Tuning Rounds= %d", current_configuration.ToString().c_str(), energy, tuning_rounds);

            // Continue auto-tuning
            if (temperature > temperature_min) {

                // Searching for new states
                if (state_itr < state_count) {
                    new_configuration = get_neighboring_config(current_configuration);
                    set_config(new_configuration);
                    LOG_INFO("Exploring - New Configuration: %s\t", new_configuration.ToString().c_str());
                }
                    // Cooling
                else {
                    temperature *= alpha;
                    LOG_INFO("Cooling TEMPERATURE: %f", temperature);
                    state_itr = 0;
                }

            } else {
                LOG_INFO("FINISHED COOLING");
                break;
            }

        } else {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }

    }

    LOG_INFO("Final Configuration = %s\t, and E(x)= %f", current_configuration.ToString().c_str(), energy);
}


void GradientDescent(int duration_in_seconds, PageMigrationPolicy init_config,
                     std::function<void(const PageMigrationPolicy &new_config)> set_config,
                     std::function<double()> get_current_config_cost) {
    // learning rate
    double alpha = 0.35;
    double delta = -0.5;

    // initial state
    auto current_configuration = init_config;

    int iter = 0;
    int tuning_period = 5;
    int max_iterations = duration_in_seconds / (tuning_period * 5);
    double curr_cost = -10000000;

    auto cap_prob = [](double p) -> double {
        if (p > 1) return 1;
        else if (p < 0) return 0;
        return p;
    };

    auto add_knob_by_delta = [&](double x) {
        if (x + delta > 1)
            x = 1;
        else
            x+= delta;
        return cap_prob(x);
    };

    while (iter < max_iterations) {
        iter++;
        set_config(current_configuration);
        std::this_thread::sleep_for(std::chrono::seconds(tuning_period));
        double prev_cost = curr_cost;
        curr_cost = get_current_config_cost();
        LOG_INFO("Current Cost: %f, Config: %s", curr_cost, current_configuration.ToString().c_str());
//        if (abs(curr_cost - prev_cost) < 1e-6) {
//            break;
//        }
        auto new_config = current_configuration;
        new_config.Dr = add_knob_by_delta(new_config.Dr);
        set_config(new_config);
        std::this_thread::sleep_for(std::chrono::seconds(tuning_period));
        double cost_delta_dr = get_current_config_cost();
        double gradient_dr = (cost_delta_dr - curr_cost) / delta;
        LOG_INFO("cost_delta_dr :: %f, config :: %s, delta :: %f, g_dr :: %f", cost_delta_dr, new_config.ToString().c_str(), delta, gradient_dr);

        new_config = current_configuration;
        new_config.Dw = add_knob_by_delta(new_config.Dw);
        set_config(new_config);
        std::this_thread::sleep_for(std::chrono::seconds(tuning_period));
        double cost_delta_dw = get_current_config_cost();
        double gradient_dw = (cost_delta_dw - curr_cost) / delta;
        LOG_INFO("cost_delta_dw :: %f, config :: %s, delta :: %f, g_dw :: %f", cost_delta_dw, new_config.ToString().c_str(), delta, gradient_dw);

        new_config = current_configuration;
        new_config.Nr = add_knob_by_delta(new_config.Nr);
        set_config(new_config);
        std::this_thread::sleep_for(std::chrono::seconds(tuning_period));
        double cost_delta_nr = get_current_config_cost();
        double gradient_nr = (cost_delta_nr - curr_cost) / delta;
        LOG_INFO("cost_delta_nr :: %f, config :: %s, delta :: %f, g_nr: %f", cost_delta_nr, new_config.ToString().c_str(), delta, gradient_nr);

        new_config = current_configuration;
        new_config.Nw = add_knob_by_delta(new_config.Nw);
        set_config(new_config);
        std::this_thread::sleep_for(std::chrono::seconds(tuning_period));
        double cost_delta_nw = get_current_config_cost();
        double gradient_nw = (cost_delta_nw - curr_cost) / delta;
        LOG_INFO("cost_delta_nw :: %f, config :: %s, delta :: %f, g_nw: %f", cost_delta_nw, new_config.ToString().c_str(), delta, gradient_nw);

        current_configuration.Dr = cap_prob(current_configuration.Dr - alpha * gradient_dr);
        current_configuration.Dw = cap_prob(current_configuration.Dw - alpha * gradient_dw);
        current_configuration.Nr = cap_prob(current_configuration.Nr - alpha * gradient_nr);
        current_configuration.Nw = cap_prob(current_configuration.Nw - alpha * gradient_nw);

        LOG_INFO("new config: %s, round %d", current_configuration.ToString().c_str(), iter);
    }

    LOG_INFO("Final Configuration = %s\t, and cost(x)= %f", current_configuration.ToString().c_str(), curr_cost);
}

}

}