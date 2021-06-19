//
// Created by zxjcarrot on 2020-01-20.
//
#include <random>
#include "util/random.h"

namespace spitfire {
void zipf(std::vector<int> &zipf_dist, double alpha, int n, int num_values) {
    static double c = 0;          // Normalization constant
    double z;          // Uniform random number (0 < z < 1)
    double sum_prob;          // Sum of probabilities
    double zipf_value = 0;          // Computed exponential value to be returned
    int i, j;

    double *powers = new double[n + 1];
    int seed = 0;
    std::mt19937 gen(seed);
    std::uniform_real_distribution<> dis(0, 1);

    for (i = 1; i <= n; i++)
        powers[i] = 1.0 / pow((double) i, alpha);

    // Compute normalization constant on first call only
    for (i = 1; i <= n; i++)
        c = c + powers[i];
    c = 1.0 / c;

    for (i = 1; i <= n; i++)
        powers[i] = c * powers[i];

    for (j = 1; j <= num_values; j++) {

    // Pull a uniform random number in (0,1)
        z = dis(gen);

    // Map z to the value
        sum_prob = 0;
        for (i = 1; i <= n; i++) {
            sum_prob = sum_prob + powers[i];
            if (sum_prob >= z) {
                zipf_value = i;
                break;
            }
        }

    //std::cout << "zipf_val :: " << zipf_value << std::endl;
        zipf_dist.push_back(zipf_value);
    }

    delete[] powers;
}
}