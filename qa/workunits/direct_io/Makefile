CFLAGS = -Wall -Wextra -D_GNU_SOURCE

TARGETS = direct_io_test test_sync_io test_short_dio_read test_short_dio_read_trunc

.c:
	$(CC) $(CFLAGS) $@.c -o $@

all:	$(TARGETS)

clean:
	rm $(TARGETS)
