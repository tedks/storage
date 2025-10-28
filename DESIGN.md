STORAGE

Storage is a meta-filesystem that multiplexes an abstract filesystem over a number of connected underlying filesystems. Unlike other network storage systems like Dropbox or Google Drive, which function like big USB sticks, or local storage systems like RAIDs, which create big USB sticks, Storage is a single filesystem that is split over mulitple different substrates. This creates a single abstract filesystem that can be backed by arbitrarily many other filesystems. 

The Storage abstract filesystem is backed by many other normal filesystems. YAML documents stored in extended attributes (synched to the underlying filesystems) control how many (or which) backing filesystems store which directories. 

Storage has a simple CLI to mount and an /etc/storagetab file to control global storage mounts.


