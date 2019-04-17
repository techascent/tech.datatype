## Types


The datatype library provides support for an extend set of primitive datatypes and
objects.  The base primitive datatypes are:

```clojure
user> (require 'tech.datatype.casting)
user> tech.datatype.casting/base-host-datatypes
#{:int32 :int16 :float32 :float64 :int64 :int8 :boolean :object}
user> tech.datatype.casting/base-datatypes
#{:int32 :int16 :float32 :float64 :int64 :uint64 :uint16 :int8 :uint32 :boolean :object
  :uint8}
```

It includes operators to change between types both at compile time and at runtime:

```clojure
user> (require '[tech.datatype.casting :as casting])
nil
user> (casting/cast 128 :int8)
Execution error (IllegalArgumentException) at tech.datatype.casting/int8-cast (casting.clj:152).
Value out of range for byte: 128
user> (casting/cast 127 :int8)
127
user> (casting/unchecked-cast 128 :int8)
-128
user> (casting/cast -128 :uint8)
Execution error (ExceptionInfo) at tech.datatype.casting/fn (casting.clj:261).
Value out of range
user> (casting/unchecked-cast -128 :uint8)
128
```


We can find out the datatype of anything in the system by querying it:


```clojure
user> (require '[tech.datatype :as dtype])
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

user> (require '[tech.datatype :as dtype])
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
Execution error (IllegalArgumentException) at tech.datatype.casting/int8-cast (casting.clj:152).
Value out of range for byte: 254
user> (def test-writer (dtype/->writer-of-type test-ary :uint8))
#'user/test-writer
user> (.write test-writer 0 128)
nil
user> test-ary
[128, 0, 1, 2, 3]
user> (.write test-writer 0 -1)
Execution error (ExceptionInfo) at tech.datatype.writer$fn$reify__38421/write (writer.clj:272).
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
Execution error (IllegalArgumentException) at tech.datatype.protocols/eval31323$fn$G (protocols.clj:153).
No implementation of method: :->mutable-of-type of protocol: #'tech.datatype.protocols/PToMutable found for class: [I
user> (dtype/->mutable-of-type (dtype/->list-backing-store test-ary) :int32)
#object[tech.datatype.mutable$fn$reify__39892 0x1f3d5040 "tech.datatype.mutable$fn$reify__39892@1f3d5040"]
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
