# intro

  1. enable implementation high perf algorithms.
  2. enable numeric computing of the type found in libraries such as numpy and languages such as APL.
  3. bridging between idiomatic clojure code and C or numeric libraries such as numpy or TVM.

  A numeric transformation of an algorithm will often yield significant performance gains.  It can yield somewhat
  significant simplification of concents and enables more tools such as Neanderthal, numpy, or high performance GPU
  algorithms to further enhance algorithm performance.

  One theory is that for the set of programs that clojure has insufficient power to express efficiently
  a more numeric definition is far less work-intensive solution than switching languages.

# datatypes
 - standard data types.
 - ecount, shape
 - get-datatype
 - casting

# containers and copying
 - java-array
 - nio-buffer
 - typed-buffer
 - native-buffer
 - list (more detail below)
 - fastcopying & times

# extending the set of datatypes
 - datatype extensions to support more object datatypes
 - aliased datatypes & custom printing of types

# scalars, iterables, and readers
 - querying conversions and converting to readers.
 - changing datatypes.

# outline of tech.v2.datatype.functional - this is where cheatsheet is just too minimal
 * lazy reader/iterable function application.
 * Table of all functions aside from purely index space functions grouped by utility.

# outline of statistics package

# discussion of working in index space - arg* functions
 - By simply generating indexes we can create datasets that are infeasibly large with existing tech.
 - arggroup-by is one of the stars here.  It has excellent performance.

# discussion of efficient indexed parallelization primitives
 - why pmap is insufficient
 - why chunking needs to be done in userspace no the library.
 - [indexed-map-reduce](https://github.com/techascent/tech.parallel/blob/master/src/tech/parallel/for.clj#L10)

# index algebra
 * 'native' mapping (increment of exactly 1)
 * single integer in this space stands for `(range int-value)`
 * select
 * offset
 * broadcast

# Efficient reader compositions
 * concat - concatenate n readers
 * const - make a reader out of a value
 * indexed - make a reader of out a reader of indexes and a reader
 * reverse - reverse a reader
 * update - use a sparse map to replace just some of the values in a reader

# Fastutil Lists
 - Very efficient for building up large datasets
 - Efficient conversion to tuple of array,offset,len

# bitmaps
 - bitmaps are specialized designed structures that work extremely well in index space
 - [RoaringBitmap](https://roaringbitmap.org/) is a real gem here
 - protocol bitmapset functions
 - interaction with index algebra

# ranges + range algebra
 - monotonic-range namespace
 - protocol methods around range algebra

# tensors
 - dimensions, dimension analysis
 - row-major discussion, native hardware mapping for ranges
 - select, rotate, broadcast, slice
 - indexing bytecode generation
 - subrect copying optimization
 - difference (and partial isomorphism) between tensors and readers.

# datetimes
 * covered in separate document
 * operators extend above.
 * function list
 * operator function list.

# buffered-images
 * covered more thoroughly in separate document
 * function list

# Appendix
```clojure
user> (require '[clj-memory-meter.core :as mm])
nil
user> (require '[tech.v2.datatype :as dtype])
nil
user> (defn measure-primitive-box-size
        [dtype]
        (let [prim-ary (dtype/make-container :java-array dtype (range 1000) {:unchecked? true})]
           {:primitive-array (mm/measure prim-ary)
            :object-array (mm/measure (object-array prim-ary))
            :datatype dtype}))
#'user/measure-primitive-box-size
user> (->> casting/host-numeric-types
           (mapv measure-primitive-box-size))
[{:primitive-array "3.9 KB", :object-array "19.5 KB", :datatype :int32}
 {:primitive-array "2.0 KB", :object-array "19.5 KB", :datatype :int16}
 {:primitive-array "3.9 KB", :object-array "19.5 KB", :datatype :float32}
 {:primitive-array "7.8 KB", :object-array "27.4 KB", :datatype :float64}
 {:primitive-array "7.8 KB", :object-array "27.4 KB", :datatype :int64}
 {:primitive-array "1016 B", :object-array "7.9 KB", :datatype :int8}]
```
