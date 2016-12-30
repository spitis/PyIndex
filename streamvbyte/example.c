#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <time.h>

#include "streamvbyte.h"
#include "streamvbytedelta.h"

#include "varintencode.h"
#include "varintdecode.h"

int test_masked(void);

int main() {
  srand(time(NULL));
  int k, N = 20;
	uint32_t * datain = malloc(N * sizeof(uint32_t));
	uint8_t * compressedbuffer = malloc(N * sizeof(uint32_t));
	uint32_t * recovdata = malloc(N * sizeof(uint32_t));
	for (k = 0; k < N; ++k)
	  datain[k] = k*8 + rand()%8;


	printf(" In: [");
	for (k = 0; k < N; ++k) {
	  printf("%d, ", datain[k]);
	}
	printf("%d]\n", datain[k-1]);

	size_t compsize = streamvbyte_delta_encode(datain, N, compressedbuffer, 0); // encoding

	// here the result is stored in compressedbuffer using compsize bytes
	size_t compsize2 = streamvbyte_delta_decode(compressedbuffer, recovdata,
						    N, 0); // decoding (fast)

	assert(compsize == compsize2);

	printf("Out: [");
	for (k = 0; k < N; ++k) {
	  printf("%d, ", recovdata[k]);
	}
	printf("%d]\n",recovdata[k-1]);

	free(datain);
	free(compressedbuffer);
	free(recovdata);
	printf("Compressed %d integers down to %d bytes.\n",N,(int) compsize);

  test_masked();
  return 0;
}

int test_masked() {
	int N = 5000;
	uint32_t * datain = malloc(N * sizeof(uint32_t));
	uint8_t * compressedbuffer = malloc(N * sizeof(uint32_t));
	uint32_t * recovdata = malloc(N * sizeof(uint32_t));
	for (int k = 0; k < N; ++k)
		datain[k] = 120;
	size_t compsize = vbyte_encode(datain, N, compressedbuffer); // encoding
	// here the result is stored in compressedbuffer using compsize bytes
	size_t compsize2 = masked_vbyte_decode(compressedbuffer, recovdata,
					N); // decoding (fast)
	assert(compsize == compsize2);
	free(datain);
	free(compressedbuffer);
	free(recovdata);
	printf("Compressed %d integers down to %d bytes.\n",N,(int) compsize);
	return 0;
}
