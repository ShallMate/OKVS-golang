#include <immintrin.h>
#include <stdint.h>

extern "C" {
    uint32_t optimized_xor(uint8_t* row, uint32_t* r_P, int W, int Pos, int Value) {
        __m256i xor_sum = _mm256_setzero_si256();
        int idx = 0;
        int j = 0;

        for (; j <= W - 8; j += 8) {
            for (int k = 0; k < 8; ++k) {
                int bit_index = (j + k) % 8;
                int byte_index = (j + k) / 8;
                if (row[byte_index] & (1 << (7 - bit_index))) {
                    idx = Pos + j + k;
                    xor_sum = _mm256_xor_si256(xor_sum, _mm256_set1_epi32(r_P[idx]));
                }
            }
        }

        uint32_t result_arr[8];
        _mm256_storeu_si256((__m256i*)result_arr, xor_sum);
        uint32_t total_xor_sum = 0;
        for (int m = 0; m < 8; ++m) {
            total_xor_sum ^= result_arr[m];
        }

        for (; j < W; ++j) {
            int bit_index = j % 8;
            int byte_index = j / 8;
            if (row[byte_index] & (1 << (7 - bit_index))) {
                idx = Pos + j;
                total_xor_sum ^= r_P[idx];
            }
        }

        total_xor_sum ^= Value;
        return total_xor_sum;
    }
}
