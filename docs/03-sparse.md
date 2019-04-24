# Sparse Storage


Sparse storage support is an important element to numeric computing systems as it allows
desktop machines to processing and work with data of very high element counts.  This
data comes in problems of loosely coupled systems and is common in NLP algorithms.  We
present a simplified but fully functional sparse storage system built upon the
concepts of the previous documents.  For a sparse item, we assume one of the values is
far, far more prevalent than other values.  The number of non-sparse elements we will
call `NS`.


## Objectives

Key objects of sparse storage are as follows:

1.  Unary operations are constant time.
2.  Binary operations are linear with respect to the union of non-sparse-indexes.
3.  Storage space is linear with respect to the non-sparse-indexes.
4.  Assignment is linear with respect to the union of non-sparse-indexes.
5.  Conversion to CSR and CSC formats is efficient, 0(NS) in the worst case.


For more background on CSR and CSC formats, see
[wikipedia](https://en.wikipedia.org/wiki/Sparse_matrix).

One way the sparse storage in tech.datatype differs from the wikipedia article is that
we allow any value to be considered the sparse value.  This allows unary operations to
be constant time and ensures the result of a binary operation will have the same sparse
properties as the initial 2 buffers assuming both inputs are themselves sparse buffers.
It also allows the concept of sparse to extend into arbitrary spaces where the sparse
value can be an arbitrary object.


## Sparse Readers And Buffers

A sparse reader is a reader but it implements a new protocol,
tech.datatype.sparse.PSparse:

```clojure
(defprotocol PSparse
  (index-seq [item]
    "Return a sequence of records of
:global-index
:data-index")
  (sparse-value [item])
  (sparse-ecount [item]
    "Number of sparse elements in this datastructure.  Constant time query.")

  (readers [item]
    "Return a map containing
{
 :indexes - int32 reader of indexes in global space.
 :data - (optional) reader of data values.
}")
  (iterables [item]
    "Return a map containing:
{
 :indexes int32 iterable of indexes in global space.
 :data iterable of values in global space.
}"))
```

It has optimized methods for read-indexes and unary, binary operations are implemented
in terms of sparse readers when the underlying buffer type is advertised as sparse.
Copying a sparse buffer to either a dense or sparse buffer is an optimized operation.
The system has multimethods defined for the operations so it will automatically detect
the buffer type and switch to the appropriate optimized implemention so aside from
allocating the sparse buffer you may not need to know at any point that it is sparse.


The sparse reader namespace has a function for definition of a const sparse reader which
is an important optimization as, for instance, multiplying sparse constant by another
sparse bufffer results in something that is O(NS) of the first buffer.


## Sparse Buffers


Allocating a sparse buffer via `(make-container :sparse & rest)` allocates a sparse
buffer that will set the sparse value to a datatype-dependent sparse value:

* `0` for numbers
* `false` for booleans
* `nil` for objects


To set a specific sparse value, use the full version of the make container protocol:

```clojure

user> (require '[tech.v2.datatype :as dtype])
:tech.resource.gc Reference thread starting
nil
user> (dtype/make-container :sparse :int32 [1 0 1 0 1 0 1])
{:b-offset 0,
 :b-elem-count 7,
 :sparse-value 0,
 :indexes [0 2 4 6],
 :data [1 1 1 1],
 :buffer-datatype :int32}
user> (dtype/make-container :sparse :int32 [1 0 1 0 1 0 1] {:sparse-value 1})
{:b-offset 0,
 :b-elem-count 7,
 :sparse-value 1,
 :indexes [1 3 5],
 :data [0 0 0],
 :buffer-datatype :int32}
```

Outside of very special cases we don't recommend setting the sparse value.  It will most
likely achieve only confusion.


The sparse buffer implements an optimized version of write-indexes! that will sort the
incoming indexes unless :indexes-in-order? is specified.  This forms the core
implementation of an optimized copy operation.  In addition, set-constant! to the sparse
value is a fairly simple operation of removing that range from the index mutable and
data mutable.


All datatypes are supported for sparse readers and sparse buffers.


## Notes


### Union

There are, unfortunately, 2 union operations.  One for assignment and one for binary
operations:
1.  For assignment, if we find matching indexes we want to take the right data value.
    Else, we want to take the data value from the lower of the two indexes.
2.  For binary operations, if the indexes match the result is a binary operation of both
    of the values.  Else the result is a binary operation of the lower value and the
    other reader's sparse value.


### Alternative Implementations

We have reason to believe there is an alternative implementation that would offer
superior performance for some subset of operations  using the
[bifurcan library](https://github.com/lacuna/bifurcan).  The argument against this
is the 2 union operations above and the ability to share data as a sparse vector
with java matrix libraries such as [ejml](https://github.com/lessthanoptimal/ejml).
