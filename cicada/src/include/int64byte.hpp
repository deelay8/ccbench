#ifndef INT64BYTE_HPP
#define INT64BYTE_HPP

#include <cstdint>

class uint64_t_64byte {
public:
	uint64_t num;
	int8_t padding[56];
};

#endif	//	INT64BYTE_HPP