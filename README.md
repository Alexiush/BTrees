# BTrees

B-tree is a search tree tailored for use in scenarios where random access is slow (e.g. the node needs to be read from the hard drive) 
to a degree where it is more beneficial to have a "fat" node that would contain many keys - it is slower to work with on the node level, but the tree becomes shorter and fewer reads/writes are made.
To maintain a reasonable height, the B-tree is being balanced by keeping its nodes at least half full (with an exception made for the root), 
so a node branches when it can (and must) split full node into two valid (half-full, "big") nodes and shrinks (merges) back when it can't maintain the ratio.

There are also variations of B-tree:
- B+, where only leaves contain values allowing nodes to be generally larger and reducing the amount of changes made to internal nodes
- B*, where the "big" ratio is set to 2/3 instead of 1/2, so the tree is denser, but is more complex because the ratio is maintained by 2-to-3 splits, 3-to-2 merges and passing the key-value pairs between the neighbours
- B+*, which combines both

These implementations of B-trees are proactive meaning that if it is possible that operation may fail and split or merge to be performed 
and to check if it is so it is required to descend further operation is performed in advance, so that no unnecessary I/O is done. 

This repository was created as a part of an assignment in CS class. 
While making it I have noticed that there is not so many resources that have data on B* and B+* tree data structures, so I decided to share. 
Thus, implementations here have diagnostics, but lack actual work with the hard drive or anything.
