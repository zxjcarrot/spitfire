//
// Created by zxjcarrot on 2020-03-27.
//

#ifndef SPITFIRE_TPCC_RECORD_H
#define SPITFIRE_TPCC_RECORD_H

#include <string>

#include "engine/txn.h"
#include "murmur/MurmurHash2.h"

namespace spitfire {
namespace benchmark {
namespace tpcc {

struct Warehouse : BaseTuple {
    int32_t W_ID;
    char W_NAME[16];
    char W_STREET_1[32];
    char W_STREET_2[32];
    char W_CITY[32];
    char W_STATE[2];
    char W_ZIP[9];
    double W_TAX;
    double W_YTD;

    int32_t Key() const {
        return W_ID;
    }
};


struct District : BaseTuple {
    int32_t D_ID;
    int32_t D_W_ID;
    char D_NAME[16];
    char D_STREET_1[32];
    char D_STREET_2[32];
    char D_CITY[32];
    char D_STATE[2];
    char D_ZIP[9];
    double D_TAX;
    double D_YTD;
    int32_t D_NEXT_O_ID;

    struct DistrictKey {
        int32_t D_W_ID;
        int32_t D_ID;

        bool operator<(const DistrictKey &rhs) const {
            return D_W_ID < rhs.D_W_ID || D_W_ID == rhs.D_W_ID && D_ID < rhs.D_ID;
        }

        bool operator==(const DistrictKey &rhs) const {
            return memcmp(reinterpret_cast<const char *>(this), reinterpret_cast<const char *>(&rhs), sizeof(*this)) ==
                   0;
        }
    };

    DistrictKey Key() const {
        return {D_W_ID, D_ID};
    }
};

struct Item : BaseTuple {
    int32_t I_ID;
    int32_t I_IM_ID;
    char I_NAME[32];
    double I_PRICE;
    char I_DATA[64];

    int32_t Key() const {
        return I_ID;
    }
};

struct Customer : BaseTuple {
    int32_t C_ID;
    int32_t C_D_ID;
    int32_t C_W_ID;
    char C_FIRST[32];
    char C_MIDDLE[2];
    char C_LAST[32];
    char C_STREET_1[32];
    char C_STREET_2[32];
    char C_CITY[32];
    char C_STATE[2];
    char C_ZIP[9];
    char C_PHONE[32];
    uint64_t C_SINCE;
    char C_CREDIT[2];
    double C_CREDIT_LIM;
    double C_DISCOUNT;
    double C_BALANCE;
    double C_YTD_PAYMENT;
    int32_t C_PAYMENT_CNT;
    int32_t C_DELIVERY_CNT;
    char C_DATA[500];

    struct CustomerKey {
        int32_t C_W_ID;
        int32_t C_D_ID;
        int32_t C_ID;

        bool operator<(const CustomerKey &rhs) const {
            return C_W_ID < rhs.C_W_ID || C_W_ID == rhs.C_W_ID && C_D_ID < rhs.C_D_ID ||
                   C_W_ID == rhs.C_W_ID && C_D_ID == rhs.C_D_ID && C_ID < rhs.C_ID;
        }

        bool operator==(const CustomerKey &rhs) const {
            return memcmp(reinterpret_cast<const char *>(this), reinterpret_cast<const char *>(&rhs), sizeof(*this)) ==
                   0;
        }
    };

    CustomerKey Key() const {
        return {C_W_ID, C_D_ID, C_ID};
    }
};


struct CustomerIndexedColumns : BaseTuple {
    int32_t C_ID;
    int32_t C_W_ID;
    int32_t C_D_ID;
    char C_LAST[32];

    bool operator<(const CustomerIndexedColumns &rhs) const {
        return C_W_ID < rhs.C_W_ID || C_W_ID == rhs.C_W_ID && C_D_ID < rhs.C_D_ID ||
               C_W_ID == rhs.C_W_ID && C_D_ID == rhs.C_D_ID && std::string(C_LAST) < std::string(rhs.C_LAST) ||
               C_W_ID == rhs.C_W_ID && C_D_ID == rhs.C_D_ID && std::string(C_LAST) == std::string(rhs.C_LAST) &&
               C_ID < rhs.C_ID;
    }

    CustomerIndexedColumns Key() const {
        return *this;
    }

    bool operator==(const CustomerIndexedColumns &rhs) const {
        return C_ID == rhs.C_ID && C_W_ID == rhs.C_W_ID && C_D_ID == rhs.C_D_ID &&
               std::string(C_LAST) == std::string(rhs.C_LAST);
    }
};


struct History : BaseTuple {
    int32_t H_ID;
    int32_t H_C_ID;
    int32_t H_C_D_ID;
    int32_t H_C_W_ID;
    int32_t H_D_ID;
    int32_t H_W_ID;
    uint64_t H_DATE;
    double H_AMOUNT;
    char H_DATA[32];

    int32_t Key() const {
        return H_ID;
    }
};

struct Stock : BaseTuple {
    int32_t S_I_ID;
    int32_t S_W_ID;
    int32_t S_QUANTITY;
    char S_DIST_01[32];
    char S_DIST_02[32];
    char S_DIST_03[32];
    char S_DIST_04[32];
    char S_DIST_05[32];
    char S_DIST_06[32];
    char S_DIST_07[32];
    char S_DIST_08[32];
    char S_DIST_09[32];
    char S_DIST_10[32];
    int32_t S_YTD;
    int32_t S_ORDER_CNT;
    int32_t S_REMOTE_CNT;
    char S_DATA[64];

    struct StockKey {
        int32_t S_W_ID;
        int32_t S_I_ID;

        bool operator<(const StockKey &rhs) const {
            return S_W_ID < rhs.S_W_ID || S_W_ID == rhs.S_W_ID && S_I_ID < rhs.S_I_ID;
        }

        bool operator==(const StockKey &rhs) const {
            return memcmp(reinterpret_cast<const char *>(this), reinterpret_cast<const char *>(&rhs), sizeof(*this)) ==
                   0;
        }
    };

    StockKey Key() const {
        return {S_W_ID, S_I_ID};
    }
};

struct Order : BaseTuple {
    int32_t O_ID;
    int32_t O_C_ID;
    int32_t O_D_ID;
    int32_t O_W_ID;
    uint64_t O_ENTRY_D;
    int32_t O_CARRIER_ID;
    int32_t O_OL_CNT;
    int32_t O_ALL_LOCAL;

    struct OrderKey {
        int32_t O_W_ID;
        int32_t O_D_ID;
        int32_t O_ID;

        bool operator<(const OrderKey &rhs) const {
            return O_W_ID < rhs.O_W_ID || O_W_ID == rhs.O_W_ID && O_D_ID < rhs.O_D_ID ||
                   O_W_ID == rhs.O_W_ID && O_D_ID == rhs.O_D_ID && O_ID < rhs.O_ID;
        }

        bool operator==(const OrderKey &rhs) const {
            return memcmp(reinterpret_cast<const char *>(this), reinterpret_cast<const char *>(&rhs), sizeof(*this)) ==
                   0;
        }
    };

    OrderKey Key() const {
        return {O_W_ID, O_D_ID, O_ID};
    }
};

struct OrderIndexedColumns : BaseTuple {
    OrderIndexedColumns() {}

    OrderIndexedColumns(const Order &o) : O_ID(o.O_ID), O_C_ID(o.O_C_ID), O_D_ID(o.O_D_ID), O_W_ID(o.O_W_ID) {}

    int32_t O_ID;
    int32_t O_C_ID;
    int32_t O_D_ID;
    int32_t O_W_ID;

    bool operator<(const OrderIndexedColumns &rhs) const {
        return O_W_ID < rhs.O_W_ID || O_W_ID == rhs.O_W_ID && O_D_ID < rhs.O_D_ID ||
               O_W_ID == rhs.O_W_ID && O_D_ID == rhs.O_D_ID && O_C_ID < rhs.O_C_ID ||
               O_W_ID == rhs.O_W_ID && O_D_ID == rhs.O_D_ID && O_C_ID == rhs.O_C_ID && O_ID < rhs.O_ID;
    }

    OrderIndexedColumns Key() const {
        return *this;
    }

    bool operator==(const OrderIndexedColumns &rhs) const {
        return !(*this < rhs) && !(rhs < *this);
    }
};


struct NewOrder : BaseTuple {
    int32_t NO_O_ID;
    int32_t NO_D_ID;
    int32_t NO_W_ID;

    struct NewOrderKey {
        int32_t NO_D_ID;
        int32_t NO_W_ID;
        int32_t NO_O_ID;

        bool operator<(const NewOrderKey &rhs) const {
            return NO_D_ID < rhs.NO_D_ID || NO_D_ID == rhs.NO_D_ID && NO_W_ID < rhs.NO_W_ID ||
                   NO_D_ID == rhs.NO_D_ID && NO_W_ID == rhs.NO_W_ID && NO_O_ID < rhs.NO_O_ID;
        }

        bool operator==(const NewOrderKey &rhs) const {
            return memcmp(reinterpret_cast<const char *>(this), reinterpret_cast<const char *>(&rhs),
                          sizeof(NewOrderKey)) == 0;
        }
    };

    NewOrderKey Key() const {
        return {NO_D_ID, NO_W_ID, NO_O_ID};
    }
};


struct OrderLine : BaseTuple {
    int32_t OL_O_ID;
    int32_t OL_D_ID;
    int32_t OL_W_ID;
    int32_t OL_NUMBER;
    int32_t OL_I_ID;
    int32_t OL_SUPPLY_W_ID;
    int64_t OL_DELIVERY_D;
    int32_t OL_QUANTITY;
    double OL_AMOUNT;
    char OL_DIST_INFO[32];

    struct OrderLineKey {
        int32_t OL_W_ID;
        int32_t OL_D_ID;
        int32_t OL_O_ID;
        int32_t OL_NUMBER;

        bool operator<(const OrderLineKey &rhs) const {
            return OL_W_ID < rhs.OL_W_ID || OL_W_ID == rhs.OL_W_ID && OL_D_ID < rhs.OL_D_ID ||
                   OL_W_ID == rhs.OL_W_ID && OL_D_ID == rhs.OL_D_ID && OL_O_ID < rhs.OL_O_ID ||
                   OL_W_ID == rhs.OL_W_ID && OL_D_ID == rhs.OL_D_ID && OL_O_ID == rhs.OL_O_ID &&
                   OL_NUMBER < rhs.OL_NUMBER;
        }

        bool operator==(const OrderLineKey &rhs) const {
            return memcmp(reinterpret_cast<const char *>(this), reinterpret_cast<const char *>(&rhs), sizeof(*this)) ==
                   0;
        }
    };

    OrderLineKey Key() const {
        return {OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER};
    }
};

//struct OrderLineIndexedColumns : BaseTuple {
//    OrderLineIndexedColumns() {}
//    OrderLineIndexedColumns(const OrderLine & ol): OL_O_ID(ol.OL_O_ID), OL_D_ID(ol.OL_D_ID), OL_W_ID(ol.OL_W_ID), OL_NUMBER(ol.OL_NUMBER) {}
//    int32_t OL_O_ID;
//    int32_t OL_D_ID;
//    int32_t OL_W_ID;
//    int32_t OL_NUMBER;
//
//    bool operator<(const OrderLineIndexedColumns &rhs) const {
//        return OL_W_ID < rhs.OL_W_ID || OL_W_ID == rhs.OL_W_ID && OL_D_ID < rhs.OL_D_ID ||
//               OL_W_ID == rhs.OL_W_ID && OL_D_ID == rhs.OL_D_ID && OL_O_ID < rhs.OL_O_ID ||
//                OL_W_ID == rhs.OL_W_ID && OL_D_ID == rhs.OL_D_ID && OL_O_ID == rhs.OL_O_ID && OL_NUMBER < rhs.OL_NUMBER;
//    }
//
//    OrderLineIndexedColumns Key() const {
//        return *this;
//    }
//};


}
}
}


namespace std {

template<>
class hash<spitfire::benchmark::tpcc::District::DistrictKey> {
public:
    size_t operator()(spitfire::benchmark::tpcc::District::DistrictKey const &c) const {
        const char *data = reinterpret_cast<const char *>(&c);
        return MurmurHash64A(data, sizeof(c), 0);
    }
};

template<>
class hash<spitfire::benchmark::tpcc::Customer::CustomerKey> {
public:
    size_t operator()(spitfire::benchmark::tpcc::Customer::CustomerKey const &c) const {
        const char *data = reinterpret_cast<const char *>(&c);
        return MurmurHash64A(data, sizeof(c), 0);
    }
};


template<>
class hash<spitfire::benchmark::tpcc::CustomerIndexedColumns> {
public:
    size_t operator()(spitfire::benchmark::tpcc::CustomerIndexedColumns const &c) const {
        const char *data = reinterpret_cast<const char *>(&c);
        size_t h1 = MurmurHash64A(data, sizeof(c) - sizeof(c.C_LAST), 0);
        return h1 ^ std::hash<std::string>()(std::string(c.C_LAST));
    }
};

template<>
class hash<spitfire::benchmark::tpcc::Stock::StockKey> {
public:
    size_t operator()(spitfire::benchmark::tpcc::Stock::StockKey const &c) const {
        const char *data = reinterpret_cast<const char *>(&c);
        return MurmurHash64A(data, sizeof(c), 0);
    }
};

template<>
class hash<spitfire::benchmark::tpcc::Order::OrderKey> {
public:
    size_t operator()(spitfire::benchmark::tpcc::Order::OrderKey const &c) const {
        const char *data = reinterpret_cast<const char *>(&c);
        return MurmurHash64A(data, sizeof(c), 0);
    }
};

template<>
class hash<spitfire::benchmark::tpcc::OrderIndexedColumns> {
public:
    size_t operator()(spitfire::benchmark::tpcc::OrderIndexedColumns const &c) const {
        const char *data = reinterpret_cast<const char *>(&c);
        return MurmurHash64A(data, sizeof(c), 0);
    }
};

template<>
class hash<spitfire::benchmark::tpcc::NewOrder::NewOrderKey> {
public:
    size_t operator()(spitfire::benchmark::tpcc::NewOrder::NewOrderKey const &c) const {
        const char *data = reinterpret_cast<const char *>(&c);
        return MurmurHash64A(data, sizeof(c), 0);
    }
};

template<>
class hash<spitfire::benchmark::tpcc::OrderLine::OrderLineKey> {
public:
    size_t operator()(spitfire::benchmark::tpcc::OrderLine::OrderLineKey const &c) const {
        const char *data = reinterpret_cast<const char *>(&c);
        return MurmurHash64A(data, sizeof(c), 0);
    }
};

}
#endif //SPITFIRE_TPCC_RECORD_H
