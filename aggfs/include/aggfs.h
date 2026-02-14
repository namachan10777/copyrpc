/*
 * aggfs.h -- C API for libaggfs (aggfs client library)
 *
 * Link with: -laggfs
 */

#ifndef AGGFS_H
#define AGGFS_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Opaque client handle. */
typedef struct FsClient FsClient;

/*
 * Connect to a aggfsd daemon via shared memory.
 * Returns NULL on failure.
 */
FsClient *aggfs_connect(const char *shm_path, size_t chunk_size);

/* Disconnect and free the client handle. */
void aggfs_disconnect(FsClient *client);

/* Create a file. Returns 0 on success, negative errno on error. */
int aggfs_create(FsClient *client, const char *path, uint32_t mode);

/* Open a file. Returns fd >= 0 on success, -1 on error. */
int64_t aggfs_open(FsClient *client, const char *path);

/* Close a file descriptor. Returns 0 on success, negative errno on error. */
int aggfs_close(FsClient *client, uint32_t fd);

/* Write at offset (pwrite). Returns bytes written, or negative errno. */
int64_t aggfs_pwrite(FsClient *client, uint32_t fd,
                         const uint8_t *buf, size_t len, uint64_t offset);

/* Read at offset (pread). Returns bytes read, or negative errno. */
int64_t aggfs_pread(FsClient *client, uint32_t fd,
                        uint8_t *buf, size_t len, uint64_t offset);

/* Stat a file. Writes size to *size_out. Returns 0 or negative errno. */
int aggfs_stat(FsClient *client, const char *path, uint64_t *size_out);

/* Unlink (remove) a file. Returns 0 on success, negative errno on error. */
int aggfs_unlink(FsClient *client, const char *path);

#ifdef __cplusplus
}
#endif

#endif /* AGGFS_H */
