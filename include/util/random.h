//
// Created by zxjcarrot on 2020-01-20.
//

#ifndef SPITFIRE_RANDOM_H
#define SPITFIRE_RANDOM_H
#include <vector>

namespace spitfire {

void zipf(std::vector<int>& zipf_dist, double alpha, int n, int num_values);
}
#endif //SPITFIRE_RANDOM_H
