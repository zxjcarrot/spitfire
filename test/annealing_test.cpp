//
// Created by zxjcarrot on 2020-03-23.
//

// ANNEALING TEST


#include <iostream>
#include <stdlib.h>
#include <cmath>
#include <random>
#include <sstream>
#include <strstream>

struct Config {
    double x = 10;
    double y = 10;
    double p = 10;
    double q = 10;
    std::string ToString() {
        std::ostringstream os;
        os << x << "," << y << "," << p << "," << q;
        return os.str();
    }
};

double get_energy(Config c)  {
    return 5 * pow(c.x, 2.0) + pow (c.y, 2.0) + pow(c.p, 2.0) + pow(c.q, 2.0);
}



void SimulatedAnnealingControlFlow(){

    std::random_device rd;
    std::mt19937 generator(rd());
    std::uniform_real_distribution<> distribution(0.0, 1.0);


    auto capping = [](double p) -> double {
        if (p > 10) return 10;
        else if (p < -10) return -10;
        return p;
    };

    auto get_neighbor = [&] (Config c) -> Config {
        Config n = c;
        int d = rand() % 4;
        double inc = (2 * distribution(generator) - 1);
        if (d == 0) {
            n.x = capping(n.x + inc);
        } else if (d == 1) {
            n.y = capping(n.y + inc);
        } else if (d == 2) {
            n.p = capping(n.p + inc);
        } else {
            n.q = capping(n.q + inc);
        }
        return n;
    };
    // cooling parameter
    double alpha = 0.9;
    const double e = 2.718281828;
    double temperature = 80;
    double temperature_min = 0.00008;
    int state_count = 10;
    int state_itr = 0;

    // initial state
    Config init_configuration = {10, 10, 10, 10};
    Config current_configuration = init_configuration;
    Config new_configuration = init_configuration;
    double energy = 0;
    double new_energy = 0;
    double acceptance_probablility = 0;

    int operation_itr = 0;
    int operation_count = 100 * 1000 * 1000;
    int init_period = 1000 * 100;
    int print_period = 1000 * 100;
    int tuning_period = print_period;

    int tuning_rounds = 0;
    while(operation_itr < operation_count){
        operation_itr++;

        // Wait some time before initialization
        if(operation_itr < init_period){
            continue;
        }

        // Initialization
        if(operation_itr == init_period){
            energy = get_energy(current_configuration);
            std::cout << "Initial Configuration = " << current_configuration.ToString() << "\t, and E(x)= " << energy << "\n";
            continue;
        }

        // Print current energy
        if(operation_itr % print_period == 0){
            std::cout << "Current Configuration = " << current_configuration.ToString() << "\t, and E(x)= " << energy << "\n";
        }

        if(operation_itr % tuning_period == 0){
            tuning_rounds++;
            new_energy = get_energy(new_configuration);
            double energy_delta = (new_energy - energy);

            if(new_energy < energy){
                acceptance_probablility = 1.0;
            }
            else {
                acceptance_probablility = pow(e, -(energy_delta)/temperature);
                std::cout << "DELTA: " << energy_delta << " AP: " << acceptance_probablility << "\n";
            }

            // Switch to new configuration

            if (new_energy < energy || (distribution(generator) <= acceptance_probablility)) {
                current_configuration = new_configuration;
                state_itr++;
                if (new_energy < energy) {
                    std::cout << "NEW (ENERGY) :: " << acceptance_probablility << " :: ";
                } else {
                    std::cout << "NEW (TRIAL)  :: " << acceptance_probablility << " :: ";
                }
                std::cout << "STATE ITR: " << state_itr << "\n";
                // Update energy
                energy = new_energy;
            }
                // Switch back to current configuration
            else {
                std::cout <<   "OLD           :: " << acceptance_probablility << " :: ";
            }

            std::cout << "Current Configuration = " << current_configuration.ToString() << "\t, and E(x)= " << energy <<  ", Tuning Rounds = " <<  tuning_rounds << "\n";

            // Continue auto-tuning
            if(temperature > temperature_min){

                // Searching for new states
                if(state_itr < state_count){
                    new_configuration = get_neighbor(current_configuration);
                    std::cout << "NEW STATE: " << new_configuration.ToString() << "\n";
                }
                    // Cooling
                else {
                    temperature *= alpha;
                    std::cout << "TEMPERATURE: " << temperature << "\n";
                    state_itr = 0;
                }

            }
            else {
                std::cout << "FINISHED COOLING \n";
                break;
            }

        }

    }

    std::cout << "Final Configuration = " << current_configuration.ToString() << "\t, and E(x)= " << energy << "\n";

}




int main() {
    SimulatedAnnealingControlFlow();
    return 0;
}