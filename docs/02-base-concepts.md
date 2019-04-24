# Base Concepts

The datatype library contains a set of concepts at its most basic level that are extend
up through all other levels.  Some familiarity with these concepts will help moving
forward.


## Types


The datatype library provides support for an extend set of primitive datatypes and
objects.  The base primitive datatypes are:

```clojure
user> (require 'tech.v2.datatype.casting)
user> tech.v2.datatype.casting/base-host-datatypes
#{:int32 :int16 :float32 :float64 :int64 :int8 :boolean :object}
user> tech.v2.datatype.casting/base-datatypes
#{:int32 :int16 :float32 :float64 :int64 :uint64 :uint16 :int8 :uint32 :boolean :object
  :uint8}
```

It includes operators to change between types both at compile time and at runtime:

```clojure
user> (require '[tech.v2.datatype.casting :as casting])
nil
user> (casting/cast 128 :int8)
Execution error (IllegalArgumentException) at tech.v2.datatype.casting/int8-cast (casting.clj:152).
Value out of range for byte: 128
user> (casting/cast 127 :int8)
127
user> (casting/unchecked-cast 128 :int8)
-128
user> (casting/cast -128 :uint8)
Execution error (ExceptionInfo) at tech.v2.datatype.casting/fn (casting.clj:261).
Value out of range
user> (casting/unchecked-cast -128 :uint8)
128
```


We can find out the datatype of anything in the system by querying it:


```clojure
user> (require '[tech.v2.datatype :as dtype])
:tech.resource.gc Reference thread starting
nil
user> (dtype/get-datatype 1)
:int64
user> (dtype/get-datatype 1.0)
:float64
user> (dtype/get-datatype (int-array (range 10)))
:int32
```

The concept of checked verse unchecked type conversion is continued to the reader and
writer protocol implementation for the set of default container types:


```clojure

user> (require '[tech.v2.datatype :as dtype])
:tech.resource.gc Reference thread starting
nil
user> (def test-ary (int-array (range 10)))
#'user/test-ary
user> test-ary
[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
user> (def test-ary (int-array [254 0 1 2 3]))
#'user/test-ary
user> (vec (dtype/->reader-of-type test-ary :uint8))
[254 0 1 2 3]
user> (vec (dtype/->reader-of-type test-ary :int8))
Execution error (IllegalArgumentException) at tech.v2.datatype.casting/int8-cast (casting.clj:152).
Value out of range for byte: 254
user> (def test-writer (dtype/->writer-of-type test-ary :uint8))
#'user/test-writer
user> (.write test-writer 0 128)
nil
user> test-ary
[128, 0, 1, 2, 3]
user> (.write test-writer 0 -1)
Execution error (ExceptionInfo) at tech.v2.datatype.writer$fn$reify__38421/write (writer.clj:272).
Value out of range
```

In this way we can engineer an algorithm in a particular numeric space and then do
inline conversions on the inputs to that algorithm to the numeric space of the
algorithm.  We also have freedom to efficiently work on some set of datatypes that do
not exist in the jvm itself; namely the unsigned types prevalent in image processing and
3d graphics.


## Containers


In an example earlier we used an int array to store data.  We can generalize this
concept into something called a container and we have generic things that we can say
about containers:

*  Constant time reader conversion.
*  Constant time writer conversion.
*  `(= value (do (.write writer idx value) (.read reader idx)))`
*  Potentially major optimizations of `memset` and `memcpy` for constant assignment and
   copy operations.
*  Barring access to above optimizations, we expect to be able to safely parallelize
writing data into the container.  If the container cannot support this, then simply
using a mutex in the implementation allows the implementation to decide the
circumstances where parallelization is safe.
*  Optionally may implement the mutation interfaces.


```clojure
user> (def test-ary (dtype/make-container :java-array :int32 (range 10)))
#'user/test-ary
user> test-ary
[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
user> (dtype/->buffer-backing-store test-ary)
#object[java.nio.HeapIntBuffer 0x666f129f "java.nio.HeapIntBuffer[pos=0 lim=10 cap=10]"]
user> (dtype/->list-backing-store test-ary)
[0 1 2 3 4 5 6 7 8 9]
user> (dtype/->mutable-of-type test-ary :int32)
Execution error (IllegalArgumentException) at tech.v2.datatype.protocols/eval31323$fn$G (protocols.clj:153).
No implementation of method: :->mutable-of-type of protocol: #'tech.v2.datatype.protocols/PToMutable found for class: [I
user> (dtype/->mutable-of-type (dtype/->list-backing-store test-ary) :int32)
#object[tech.v2.datatype.mutable$fn$reify__39892 0x1f3d5040 "tech.v2.datatype.mutable$fn$reify__39892@1f3d5040"]
```

We expect to be able to move data between disparate container types as the system will
transparently create the correct reader/writer pair and parallelize the operation.
Given containers of the same datatype we expect to have times comparable or better to
calling memset and memcpy in C for set-constant! and copy! operations when applicable as
the system will bypass the reader/writer pair and use the memcpy for native or
native-and-jvm operation types and arraycopy for purely jvm operation types.


The base builtin container types are:

* `:typed-buffer` - java-array-backed storage without mutable ability
* `:list` - java-array-backed storage with mutable ability
* `:native-buffer` - C-malloc-backed storage without mutable ability
* `:sparse` - Sparse storage simulating storage without mutable ability

All containers implement the clojure.core.matrix.protocols/PElementCount protocol.


Containers have both a container type and a buffer type.  The container type is the
what we discussed above.  The buffer type is a more general definition and exists
to indicate the type of algorithm operations upon the container (or a reader made
from the container) should use.

```clojure
user> (dtype/buffer-type test-data)
:dense
```

Containers also tend to be constant-time convertible to each other.

```clojure
user> (dtype/as-list test-data)
[0 1 3 3 4 5 6 7 8 9]
user> (type *1)
it.unimi.dsi.fastutil.ints.IntArrayList
user> (dtype/as-array test-data)
{:offset 0, :length 10, :java-array [0, 1, 3, 3, 4, 5, 6, 7, 8, 9, 0, 0, 0]}
user> (dtype/as-nio-buffer test-data)
#object[java.nio.HeapIntBuffer 0x688f199f "java.nio.HeapIntBuffer[pos=0 lim=10 cap=13]"]
```

Almost anything is convertible to nio buffers, but only things with a conversion to
as-array are convertible to lists.  Lists are convertible to both arrays and nio buffers
but mutations to the list struct (insert/remove) are of course not visible to the
converted items.  In this way you can create a 'snapshot' of the list at a given time.
Note however that read/writes into the list *will* potentially be visible to other
buffers assuming an add/remove operation does not cause the list to internally reallocate
its base storage.  So, use with care.  When in doubt, copy.


As mentioned before, things copy to other things.  Generically copy just expects the src
to be reader-convertible and the dest to be writer-convertible and it will figure out
the best thing to do from there.  Copy always returns the destination.  As a general
rule throughout the library, if a function modifies one thing it returns the modified
thing.


```clojure
user> (dtype/make-container :list :float32 (repeat 5 11))
[11.0 11.0 11.0 11.0 11.0]
user> (dtype/copy! *1 0 test-data 0 5)
[11 11 11 11 11 5 6 7 8 9]
```


## Readers And Writers


As seen above, we can create readers and writers from many things.  When combined with
operators, we can create readers and iterables that perform the operation on read/
iterate.  This provides a lazy but non-caching means of chaining together operations.

All readers implement Iterable by definition and thus you can convert any reader into a
persistent vector and they are sequable.  They also implement the element count protocol
and a very small portion of the clojure.lang.IFn interface.

```clojure
user> (def test-data (dtype/make-container :list :int32 (range 10)))
#'user/test-data
user> (vec (dtype/->reader-of-type test-data))
[0 1 2 3 4 5 6 7 8 9]
user> (vec (dtype/->reader-of-type test-data :float32))
[0.0 1.0 2.0 3.0 4.0 5.0 6.0 7.0 8.0 9.0]
user> (seq? (dtype/->reader-of-type test-data :float32))
false
user> (seq (dtype/->reader-of-type test-data :float32))
(0.0 1.0 2.0 3.0 4.0 5.0 6.0 7.0 8.0 9.0)
user> (def test-reader (dtype/->reader-of-type test-data :float32))
#'user/test-reader
user> (def test-writer (dtype/->writer-of-type test-data :int32))
#'user/test-writer
user> (dtype/ecount test-reader)
10
user> (dtype/shape test-reader)
[10]
user> (test-reader 2)
2.0
user> (test-writer 2 3)
nil
user> (test-reader 2)
3.0
```

There are utilty methods to create indexed readers and indexed writers as well as
range readers or const readers (readers that always return a constant value) and
to reverse a reader in constant time.

Once we introduce the sparse system, a better way to create a constant reader is to
use the sparse reader's make-const-reader funtion as this creates a sparse object
which can communicate more information to operations that receive it.

Readers maintain their buffer type if possible:

```clojure
user> (dtype/buffer-type test-reader)
:dense
user> (def sparse-buf (dtype/make-container :sparse :int32 [0 1 0 1 0 1 0 1]))
#'user/sparse-buf
user> sparse-buf
{:b-offset 0,
 :b-stride 1,
 :b-elem-count 8,
 :sparse-value 0,
 :indexes [1 3 5 7],
 :data [1 1 1 1],
 :buffer-datatype :int32}
user> (dtype/->reader-of-type sparse-buf)
#object[tech.v2.datatype.sparse.reader$fn$reify__48659 0x50739307 "tech.v2.datatype.sparse.reader$fn$reify__48659@50739307"]
user> (def sparse-reader *1)
#'user/sparse-reader
user> (dtype/buffer-type sparse-buf)
:sparse
user> (dtype/buffer-type sparse-reader)
:sparse
```

This allows us to apply algorithmic optimizations to the reader when used in operations.

## Operations

As mentioned in the earlier document, there are many classes of somewhat optimized
operations.

Without going through and itemizing all of the various details, one thing to note is
that you can create an operation and map it to a reader (or two readers) inline and this
operation and the mapping will be lazy and fully typed:

```clojure
user> (require '[tech.v2.datatype.unary-op :as unary-op])
nil
user> (vec test-reader)
[0.0 1.0 3.0 3.0 4.0 5.0 6.0 7.0 8.0 9.0]
user> (unary-op/unary-reader :int32 (+ x 10) test-reader)
#object[tech.v2.datatype.unary_op$fn$reify__43534 0x678f64f1 "tech.v2.datatype.unary_op$fn$reify__43534@678f64f1"]
user> (vec *1)
[10 11 13 13 14 15 16 17 18 19]
```

One thing to note is that the actual application of the operators is specialized based
on the buffer type of reader or writer


## Functional

Given anything convertible to a reader, we can apply operations defined in the
unary-op/binary-op/boolean-op/reduce-op namespaces.  The tech.v2.datatype.functional
namespace contains the core mappings that bind the builtin operations to an
apply function specialized for each operation.


```clojure
user> (require '[tech.v2.datatype.functional :as dtype-fn])
nil
user> (dtype-fn/+ 5 (range 10))
#object[tech.v2.datatype.unary_op$fn$reify__43416 0x33bbc29b "tech.v2.datatype.unary_op$fn$reify__43416@33bbc29b"]
user> (vec *1)
[5 6 7 8 9 10 11 12 13 14]
user> (dtype-fn/+ (float-array (range 10)) (dtype/make-container :list :int32 (range 10 0 -1)))
#object[tech.v2.datatype.binary_op$fn$reify__44914 0x4ca2f978 "tech.v2.datatype.binary_op$fn$reify__44914@4ca2f978"]
user> (vec *1)
[10.0 10.0 10.0 10.0 10.0 10.0 10.0 10.0 10.0 10.0]

user> (dtype-fn/> (float-array (range 10)) (dtype/make-container :list :int32 (range 10 0 -1)))
#object[tech.v2.datatype.boolean_op$fn$reify__48175 0x7b8e463f "tech.v2.datatype.boolean_op$fn$reify__48175@7b8e463f"]
user> (vec *1)
[false false false false false false true true true true]
```

Using this interface for scalars or small known operations is not advised as it is far
slow than just using clojure's built in operators even when boxing.  It is, however,
general and extends to the tensor operations later.


The rules of the return type of the operation are applied in order as follows:

1.  If any of the arguments are iterable, an iterable is returned
2.  If any are readers, a reader is returned
3.  A scalar is returned


These rules are setup to give the most flexibility possible to downstream operations
while maintaining laziness.

```clojure
user> (def test-data (float-array (range 10 0 -1)))
#'user/test-data
user> test-data
[10.0, 9.0, 8.0, 7.0, 6.0, 5.0, 4.0, 3.0, 2.0, 1.0]
user> (dtype-fn/argsort test-data)
[9, 8, 7, 6, 5, 4, 3, 2, 1, 0]
user> (dtype-fn/indexed-reader *1 test-data)
#object[tech.v2.datatype.reader$fn$reify__36268 0x906c535 "tech.v2.datatype.reader$fn$reify__36268@906c535"]
user> (vec *1)
[1.0 2.0 3.0 4.0 5.0 6.0 7.0 8.0 9.0 10.0]
user> (def indexed-reader *2)
#'user/indexed-reader
user> (vec indexed-reader)
[1.0 2.0 3.0 4.0 5.0 6.0 7.0 8.0 9.0 10.0]
user> (dtype-fn/binary-search indexed-reader 5)
[true 4]
user> (dtype-fn/binary-search indexed-reader 10)
[true 9]
user> (dtype-fn/binary-search indexed-reader 11)
[false 10]
user> (dtype-fn/binary-search indexed-reader 0)
[false 0]
```


## Sparse


The datatype library contains support for a particular type of sparse buffer.  This
buffer stores values in one mutable container and sorted indexes in another.  This
particular format was chosen due to a couple reasons.  The first, as mentioned before,
is efficient conversion to CSR and CSC formats for interoperability with external
libraries.  The second is that the above format makes range queries very efficient when
combined with binary search of the exact semantics described earlier.


We will go into depth about sparse in another document so here we just list our
expectations when dealing with sparse buffers:

1.  Unary operations are constant time.
2.  Binary operations are linear with respect to the union of non-sparse-indexes.
3.  Storage space is linear with respect to the non-sparse-indexes.
4.  Assignment is linear with respect to the union of non-sparse-indexes.
5.  Conversion to CSR and CSC formats is efficient, 0(NS) in the worst case.



## In Closing


This is a quick walkthrough of the base functionality of the datatype system.  We
haven't convered the tensor extensions and we haven't covered sparse in depth.  We have,
however, covered enough of the core concepts so as to allow for a fairly complete
understanding barring the tensor extension.
