# Hist Prototype

[![CI](https://github.com/chiefnoah/hist-prototype/actions/workflows/actions.yaml/badge.svg)](https://github.com/chiefnoah/hist-prototype/actions/workflows/actions.yaml)

A prototype of a database B+Tree index that keeps the history of all key writes.

## Architecture

The system will eventually consist of 4 main parts:

* An in-memory index
* A persisted block searcher
  * One for the index, one for the value log
* A single-threaded IO handler for both the indexer and 
* Value Logger (as in WAL, not observability tooling)
  * This will have it's own IO handler as well, generally, you'll want an IO Handler for each file.
  * This could be implemented in a variety of ways
  * Likely the first attempts will be an uncompressed, append-only file
  * Could be reworked into content-addressed storage later
  * Compression is necessary for a "real" system

### Indexer

The indexer holds an in-memory representation of the search index. It may be implemented as a B+Tree, but could also easily be swapped out for an algorithm that has better performance for in-memory queries. I'm partial to hashmaps. Currently, we just have a modified B+Tree.

### Block Searcher

The block searcher is responsible for performing searches against persisted index blocks. For most non-huge datasets, this will only be used for historical queries. Systems with low memory or huge datasets may need to read lower levels of the index into memory from disk (depending on the index implementation).

### IO Handler

The IO Handler receives messages to persist or read blocks of memory from a file. There will be different implementations depending on the type of file the handler is for (ie. indexes, values, etc.). The IO Handler may also be responsible for caching blocks in memory, though this is not planned for the prototype.