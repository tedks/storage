# STORAGE

# Overview 

Storage is a meta-filesystem that multiplexes an abstract filesystem over a number of connected underlying filesystems. Unlike other network storage systems like Dropbox or Google Drive, which function like big USB sticks, or local storage systems like RAIDs, which create big USB sticks, Storage is a single filesystem that is split over mulitple different substrates. This creates a single abstract filesystem that can be backed by arbitrarily many other filesystems, whether they are local or remote. 

The Storage abstract filesystem is backed by many other normal filesystems. YAML documents stored in extended attributes (synched to the underlying filesystems) control how many (or which) backing filesystems store which directories. Storage uses extended attributes to store all data about files, and can optionally replicate the extended attributes in a SQLite database. Generally, the philosophy of Storage is to try to store as much as possible on the filesystem itself, but the sqlite database can be used to improve performance or to handle situations where the underlying filesystems are exhausted of space or inode capacity. 

# Motivations & Goals
The main use case of Storage is to mount multiple remote filesystems of different types, syncing whole files whenever part of them is accessed and storing the files in a local mirror of the abstract filesystem which is managed like an LRU cache according to the configured size of the Storage local store. This allows for offline usage of remote resources. 

A secondary use case of Storage might be to maintain a single abstract filesystem backed by multiple RAIDs. In this case, the goal is not to have a local cache of a remote store, but to create an abstract filesystem that has storage across several local filesystems. In this case, Storage functions somewhat like a "raid" of filesystems, with the main difference that the replication behavior of files and directories is controlled by extended attributes on specific files and directories, allowing your most critical files to be replicated while other files might not be.

A third use case of Storage might be to have all of the above, but also use cloud services for certain critical files. Storage can be configured to transparently encrypt these files for privacy, while also backing up the keys to different cloud storage providers (or not). 

Finally, storage can also be configured to re-compute a file from a program invocation specified in extended attributes. These files will be garbage collected if space is required, but can be reconstructed later. This might include BitTorrent downloads, downloads from systems like Walrus, Filecoin, or IPFS, or binaries compiled from source code. 

# Configuration

## Replicas

Directories or files can be configured to be replicated N-many times. Storage will attempt to keep N-many copies of the file across different backing filesystems and will raise an error if this is impossible for some reason (because Storage will try to optimistically return from read or write system calls as soon as possible, this comes in the form of logging and putting the entire filesystem into an error state). Replica configuration applies recursively down the directory tree, and the replica configuration selected is the maximum along any node in any path through the filesystem. For example, if /foo/bar/baz has a replica configuration of 3, and /foo/bar is given a replica configuration of 4, this overrides the replica configuration for baz, and all other files under /foo/bar. A utility `storage-replicate` allows easy access to replica configuration. Replication can also be set to -1, to indicate that the file or directory should be replicated across all backing filesystems in the abstract filesystem. 

## Recomputation

Some files or directories might be able to be recomputed from a program invocation. These files (and only these files) can be marked as okay for deletion in the event the entire abstract filesystem is low on space. This is convenient for things like well-seeded BitTorrent files or files stored on persistent distributed systems that are very unlikely to ever be deleted. 

## Encryption

Files or directories can be configured to be transparently encrypted, either entirely, or for certain backing filesystems. Backing filesystems can also be configured to transparently encrypt everything written to them in the Storage configuration (as in, the central Storage configuration of the abstract filesystem, not the extended attributes of a particular file). 

# Read Path

Storage will prefer to read from a local cache over a remote filesystem in all cases. If the local cache doesn't contain the file, Storage will try to resolve the specific read request as rapidly as possible, but will also download the full file in the background. If all backing filesystems of a file are local (non-networked), Storage will try to multiplex reads to the fastest resource. If a file does not exist on any backing filesystem, but has an extended attribute specifying how the file can be recomputed, Storage will recompute the file (this can be configured to have a timeout to prevent applications from hanging while this occurs). 

## Cache Eviction

Storage can allocate (as configured) some amount of local cache space to mirror content from remote filesystems. If some backing filesystems are remote and some are local, the local filesystem serves as this local cache unless an additional local cache on a different path is configured. Storage manages this cache as an LRU cache over whole files. A read anywhere in a file causes the whole file to be downloaded to the cache, evicting the least recently used (reads and writes count). Storage will not evict a file that has not yet been synchronized as per the configuration of the file or directory and will fail to read the file that would have evicted un-flushed changes if this occurs, unless there is a file that can be recomputed that can be deleted to clear space (if this is what prevents the file from being successfully replicated). 

# Write Path

Storage will always write to and reliably fflush and similar the local cache (or most performant local filesystem) to ensure that a copy is stored at least one place first. Then, Storage will sync files as configured across backing filesystems in the background. Storage buffers writes to disconnected remote filesystems and performs them in the background upon reconnection. Storage can be configured to transparently encrypt files written to backing filesystems, though this is primarily useful in the case of cloud storage and warnings are issued if local storage is configured for transparent encryption. 

## Write Conflicts

Storage tracks write times and checksums to determine when a conflict occurs, which is when a write is about to be propagated to a filesystem where the existant file has a different checksum than the local file prior to the write. In this situation, Storage doesn't attempt to resolve the conflict in any way, but instead exposes both files to the user, with the remote file showing in the Storage abstract filesystem as "${filename}.remote.${write_timestamp}, and the local file showing as "${filename}.local.${write_timestamp}. The local file is propagated to the backing filesystems as configured under the new name, so that in the event of a local failure, the file content is not lost. The utility `storage-resolve` supports viewing and selecting canonical files, or performing common types of merges like a three-way merge for text files. 

## Free Space Management

In the event that storage across the entire abstract filesystem is not enough for continuing writes, Storage will attempt to clear space. First, Storage will attempt to find the smallest recomputable file whose deletion could create enough free space for the write, and delete it. Next, Storage looks for files whose replication counts exceed their configuration, and delete the smallest file from the slowest backing filesystem where this is the case. Finally, Storage looks for files it can move from faster backing filesystems to slower backing filesystems such to "defrag" enough to have enough space to make all the writes it needs to satisfy the replication configuration. This allows you to use slower and less private cloud storage as a "storage of last resort" to enable writes to continue. Finally, if none of these are possible, Storage will fail the write. If the write was able to occur in a local cache, but there is not enough remote space to store enough replicas, Storage logs an error and puts the abstract filesystem into an error state.

In all cases when storage "deletes" a file, it does not remove it from the filesystem, but rather rewrites it as an empty file, to preserve the configuration of the file in extended attributes. 