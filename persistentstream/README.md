How does the API look?

```
channel(backup_path, channel_size, max_file_size, max_file_count) -> (sender, receiver)
```

`Sender` is a wrapper over Tokio channel sender where `Receiver` is
tokio channel receiver

-------------------------------------

Orchestration
-------------

Important orchestration parts

- In memory channel
- Current write file
- Current read file
- Backlog file ids -> Sorted list of ids of backlog files
- Current read file index -> Index of current read file in backlog ids

We start by reading the list of backlog files during boot. These are not
indexed and we iterate through backup dir to get the list of all backlog
file ids.

The name of the next write file is created by incrementing the name of last
backlog file. Reader part of the sender operates independently using
last read file. Last read file should not mutate backlog files to
prevent races.

-----------------------------------------

To keep the receiver simple and pluggable into completely async
ecosystems, all the disk-memory orchestration happens in the sender.
Receiver is a vanilla tokio channel receiver

What does sender do?

- Writes data into inmemory channel (with a certain size)
- When the channel is full, the failed element in the current iteration
  is stored in state and `slow_receiver` flag is enabled. Let's call
  this element F1
- Next subsequent `send`s will write to disk. Let's call these elements Fn
- Immediately after writing to disk, tries to send F1 to channel. If
  successful. Sends Fn elements to channel. The last failed element becomes F1
- When all the elements in the disk is done, `slow_receiver` flag is
  toggled back

--------------------------------------

The problem with all the logic in the sender is, who wakes up the sender
when it goes inactive and the channel has free slots?

E.g Channel is full and the sender starts writing to disk and there in
no new data. Receiver received some data and the channel has slots. Who
wakes up the sender to get data from the disk and put it into channel?

Solution: Expose `sync()` method which tries to read data from disk

--------------------------------------


Loose oldest data during reboot

This enables disk orchestration with mpsc instead of mpmc as the writer
thread doesn't have to pull the data back from the channel to write to
disk.

Most of the logic in sender. This makes the vanilla receiver pluggable
into async ecosystem without complicating the codebase

--------------------------------------


