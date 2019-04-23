## Setup

```clojure
(require '[tech.v2.datatype :as dtype])
```

## Supported Datatypes

Support for the standard C datatypes and conversions between them.


```clojure
user> (require '[tech.v2.datatype.casting :as casting])
nil
user> casting/base-datatypes
#{:int32 :int16 :float32 :float64 :int64 :uint64 :uint16 :int8 :uint32 :boolean :object
  :uint8}
user> (casting/cast -1 :int8)
-1
user> (casting/cast -1 :uint8)
Execution error (ExceptionInfo) at tech.v2.datatype.casting/fn (casting.clj:266).
Value out of range for uint8: -1
user> (casting/unchecked-cast -1 :uint8)
255

```

## Containers

Make any container you like and move data in between containers fluidly.


```clojure
user> (dtype/make-container :typed-buffer :float32 5)
{:datatype :float32, :backing-store [0.0, 0.0, 0.0, 0.0, 0.0]}
user> (dtype/make-container :typed-buffer :float32 (range 5))
{:datatype :float32, :backing-store [0.0, 1.0, 2.0, 3.0, 4.0]}
user> (dtype/make-container :native-buffer :float32 (range 5))
#object[java.nio.DirectFloatBufferU 0x6156b247 "java.nio.DirectFloatBufferU[pos=0 lim=5 cap=5]"]
user> (dtype/make-container :sparse :float32 [1 0 1 0 1])
19-04-22 19:50:08 chrisn-lt-2 INFO [tech.jna.timbre-log:8] - Library c found at [:system "c"]
{:b-offset 0,
 :b-elem-count 5,
 :sparse-value 0.0,
 :indexes [0 2 4],
 :data [1.0 1.0 1.0],
 :buffer-datatype :float32}


user> (dtype/as-list (dtype/make-container :typed-buffer :uint8 5))
[0 0 0 0 0]
user> (type *1)
it.unimi.dsi.fastutil.bytes.ByteArrayList

user> (dtype/as-nio-buffer (dtype/make-container :typed-buffer :uint8 5))
#object[java.nio.HeapByteBuffer 0xac2f857 "java.nio.HeapByteBuffer[pos=0 lim=5 cap=5]"]

(dtype/->sub-array (dtype/make-container :typed-buffer :uint8 5))
{:offset 0, :length 5, :java-array [0, 0, 0, 0, 0]}
user> (type (:java-array *1))
[B

(dtype/set-value! (dtype/make-container :typed-buffer :uint8 5) 2 138)
nil
user> (dtype/set-value! (dtype/make-container :typed-buffer :uint8 5) 2 138)
{:datatype :uint8, :backing-store [0, 0, -118, 0, 0]}
user> (dtype/set-value! (dtype/make-container :typed-buffer :int8 5) 2 120)
{:datatype :int8, :backing-store [0, 0, 120, 0, 0]}
user> (dtype/get-value (dtype/make-container :typed-buffer :int8 5) 2)
0
user> (dtype/get-value (dtype/make-container :typed-buffer :int8 (range 10)) 2)
```

## Copy

Heavily optimized copying for bulk transfers.   Use this also for completing
lazy operations.


```clojure
user> (dtype/copy! (vec (range 10)) (float-array 10))
[0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]
user> (type *1)
[F
```


## Math

A small sample of what is available.  Basic elementwise operations along
with some statistics and a fixed rolling windowing facility.

* If the arguments contain an iterable, an iterable is returned.
* If the arguments contain a reader, a reader is returned.
* Else a scalar is returned.
* All arguments are casted to the widest datatype present.
* Readers implement List and RandomAccess so they look like persistent vectors in the repl.


```clojure
user> (require '[tech.v2.datatype.functional :as dtype-fn])
nil
user> (def test-data (dtype-fn/+ (range 10 0 -1) 5))
#'user/test-data
user> (dtype-fn/argsort test-data)
[9, 8, 7, 6, 5, 4, 3, 2, 1, 0]

user> test-data
(15.0 14.0 13.0 12.0 11.0 10.0 9.0 8.0 7.0 6.0)
user> (dtype-fn/argsort test-data)
[9, 8, 7, 6, 5, 4, 3, 2, 1, 0]

;; Make sequence a vector to allow indexing
user> (dtype-fn/indexed-reader *1 (vec test-data))
[6.0 7.0 8.0 9.0 10.0 11.0 12.0 13.0 14.0 15.0]

;; Inline declare a fully typed function to perform producing a new reader.

user> (dtype-fn/unary-reader :float32 (* x 3) *1)
[18.0 21.0 24.0 27.0 30.0 33.0 36.0 39.0 42.0 45.0]

user> (dtype-fn/binary-reader :float32 (+ x y) *1 *1)
[36.0 42.0 48.0 54.0 60.0 66.0 72.0 78.0 84.0 90.0]


;; Readers all support creating a reader of another type from
;; the current reader:
user> (dtype/->reader-of-type *1 :int32)
[36 42 48 54 60 66 72 78 84 90]


;; Kixi.stats is used for stats.  We also include apache commons-math3
;; so it is available for anything kixi doesn't provide.
user> (dtype-fn/mean *1)
63.0

;; All stats are lifted into the functional namespace so you don't need to
;; include this file specifically.
user> (ns-publics 'tech.v2.datatype.statistics)
{kurtosis #'tech.v2.datatype.statistics/kurtosis,
 skewness-population #'tech.v2.datatype.statistics/skewness-population,
 skewness #'tech.v2.datatype.statistics/skewness,
 standard-deviation-population
 #'tech.v2.datatype.statistics/standard-deviation-population,
 mean #'tech.v2.datatype.statistics/mean,
 harmonic-mean #'tech.v2.datatype.statistics/harmonic-mean,
 standard-error #'tech.v2.datatype.statistics/standard-error,
 variance-population #'tech.v2.datatype.statistics/variance-population,
 spearmans-correlation #'tech.v2.datatype.statistics/spearmans-correlation,
 kurtosis-population #'tech.v2.datatype.statistics/kurtosis-population,
 geometric-mean #'tech.v2.datatype.statistics/geometric-mean,
 variance #'tech.v2.datatype.statistics/variance,
 kendalls-correlation #'tech.v2.datatype.statistics/kendalls-correlation,
 standard-deviation #'tech.v2.datatype.statistics/standard-deviation,
 median #'tech.v2.datatype.statistics/median,
 pearsons-correlation #'tech.v2.datatype.statistics/pearsons-correlation}
user> (dtype-fn/variance *3)
9.166666666666666
user> (dtype-fn/fixed-rolling-window 3 dtype-fn/min test-data)
(14.0 13.0 12.0 11.0 10.0 9.0 8.0 7.0 6.0 6.0)
user> (dtype-fn/fixed-rolling-window 3 dtype-fn/max test-data)
(15.0 15.0 14.0 13.0 12.0 11.0 10.0 9.0 8.0 7.0)
user> (dtype-fn/fixed-rolling-window 3 #(apply + %) test-data)
(44.0 42.0 39.0 36.0 33.0 30.0 27.0 24.0 21.0 19.0)
```


## Tensors

Generic N-dimensional support built on top of readers.  The combination of
(something convertible to) a reader/writer *and* a dimension object gets
you a tensor.


```clojure
user> (require '[tech.v2.tensor :as tens])
nil
;; Make a tensor out of raw data.  The default datatype is double.
user> (println (tens/->tensor (partition 3 (range 9))))
#tech.v2.tensor<float64>[3 3]
[[0.000 1.000 2.000]
 [3.000 4.000 5.000]
 [6.000 7.000 8.000]]
nil


;; Make a tensor out of a reader
user> (println (tens/reshape (vec (range 9)) [3 3]))
#tech.v2.tensor<object>[3 3]
[[0 1 2]
 [3 4 5]
 [6 7 8]]


;; Cloning allows you to efficiently complete a tensor reader sequence
user> (def added-tens (dtype-fn/+ (tens/reshape (vec (range 9)) [3 3]) 2))
#'user/added-tens

;; added-tens in this case just contains a reader that will lazily evaluate the
;; above expression when necessary.
user> (println added-tens)
#tech.v2.tensor<object>[3 3]
[[2.000 3.000  4.000]
 [5.000 6.000  7.000]
 [8.000 9.000 10.000]]
nil

;; Because it is pure lazy reader, you can't write to this tensor.
user> (tens/mutable? added-tens)
false

;; But now we clone, which creates a new tensor and forces the operation.
;; There is also tensor-force which does different things for dense or sparse.
user> (def completed (tens/clone added-tens))
#'user/completed
user> (tens/mutable? completed)
true
user> (def forced (tens/tensor-force added-tens))
#'user/forced
user> (tens/mutable? forced)
true


user> (def test-tens (tens/->tensor (partition 3 (range 9))))
#'user/test-tens
user> (println (tens/transpose test-tens [1 0]))
#tech.v2.tensor<float64>[3 3]
[[0.000 3.000 6.000]
 [1.000 4.000 7.000]
 [2.000 5.000 8.000]]
nil
user> (println (tens/rotate test-tens [1 0]))
#tech.v2.tensor<float64>[3 3]
[[6.000 7.000 8.000]
 [0.000 1.000 2.000]
 [3.000 4.000 5.000]]
nil
user> (println (tens/select test-tens [1 0] :all))
#tech.v2.tensor<float64>[2 3]
[[3.000 4.000 5.000]
 [0.000 1.000 2.000]]
nil
user> (println (dtype-fn/+ 2 (tens/select test-tens [1 0] :all)))
#tech.v2.tensor<float64>[2 3]
[[5.000 6.000 7.000]
 [2.000 3.000 4.000]]
user> (tens/->jvm test-tens)
[[0.0 1.0 2.0] [3.0 4.0 5.0] [6.0 7.0 8.0]]
user> (tens/->jvm test-tens :datatype :int32
                  :base-storage :java-array)
[[0, 1, 2] [3, 4, 5] [6, 7, 8]]
user> (tens/->jvm test-tens :datatype :int32
                  :base-storage :persistent-vector)
[[0 1 2] [3 4 5] [6 7 8]]


;; matrix multiply uses blas via jna.  Anything dense will go the blas path
;; as the cost to force or complete somthing is roughly O(N) while the cost
;; of the matrix multiply is O(N*M) or roughly O(N^2).  So we can afford
;; to copy the data in order to make the matmul efficient.
;; That being said, if you are going to do a lot of these it would be wise
;; to force the inputs outside of the matrix multiply method.
user> (println (tens/matrix-multiply test-tens test-tens))
19-04-22 20:46:12 chrisn-lt-2 INFO [tech.jna.timbre-log:8] - Library blas found at [:system "blas"]
#tech.v2.tensor<float64>[3 3]
[[15.000 18.000  21.000]
 [42.000 54.000  66.000]
 [69.000 90.000 111.000]]
nil

;; matrix-multiply takes an option scalar alpha that is multiplied into the result.
user> (println (tens/matrix-multiply test-tens test-tens 3.0))
#tech.v2.tensor<float64>[3 3]
[[ 45.000  54.000  63.000]
 [126.000 162.000 198.000]
 [207.000 270.000 333.000]]
nil
```
