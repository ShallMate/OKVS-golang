#include <immintrin.h> // AVX
#include <stdint.h>    // C 标准头文件
#include <cstdio>      // 用于调试输出

extern "C" void xor_shift_simd(uint8_t* result, uint8_t* arr1, uint8_t* arr2, int shifts, int shiftnum) {
    int i;
    // 用 AVX 处理大块数据
    for (i = 0; i <= shifts - 32; i += 32) {
        __m256i vec1 = _mm256_loadu_si256((__m256i*)&arr1[i + shiftnum]);
        __m256i vec2 = _mm256_loadu_si256((__m256i*)&arr2[i]);
        __m256i res = _mm256_xor_si256(vec1, vec2);
        _mm256_storeu_si256((__m256i*)&result[i], res);
    }

    // 处理剩余部分
    for (; i < shifts; i++) {
        result[i] = arr1[i + shiftnum] ^ arr2[i];
    }
}