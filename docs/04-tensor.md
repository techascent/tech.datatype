# Tensors


A core assertion made in the initial paper was the extension of the above concepts to a
classical n-dimensions numeric computing language of the type of J or APL.  We now
demonstrate our proof of this assertion.  For our proof, we will mainly talk about the
functional (reader) part of the tensor system.  The writer part, which is used more
rarely and is less powerful, follows logically from same principles.


## Extension To N-Dimensions


Starting from a random access reader interface, our goal is to create a system that
allows computation to occurn in a N-dimensional system.  In essence, where most libraries
start from a concrete known storage datatype such as a primitive array, we start from
an abstract interface.  Given that all memory access can be modeled in this fashion
we then note that we must be able to present an n-dimensional interface upon this
basis.


We can design our extension such that it only presents a remapping of the linear
address space.  We will need the inverse of this operation for sparse tensors so the
mapping needs to be invertible.  All reorganizations of the data can be represented by
this remapping of the address space and we present 4 of them:

1.  `reshape` - starting at same base address, change the shape of the item in a way
    independent of the previous shape but respecting previous strides.
2.  `select` - Select a subset of the hyper rectangle to operate on.  This subset may or
     may not be contiguous.
3.  `transpose` - Reorder the dimensions to effect a generalized in-place transposition.
4.  `broadcast` - Duplicate some dimensions in a known way.  For our purposes, the
     duplicated dimension must evenly divide a.k.a. be commensurate with the broadcast
     dimension.
5.  `rotation` - rotate a dimension forward or backward.


We represent our mapping as a tuple of [shape strides] where shape and stride are an
array of integers and both are of the same length.  Stride represents the amount to 'skip'
between successive elements of the same dimension.  Thus a 2x2 matrix would have
dimensions of `{:shape [2 2] :strides [2 1]}`.  An in-place transpose of `[1 0]` of this
results in a dimension object of `{:shape [2 2] :strides [1 2]}`.


Mapping from global to local given an input address of n-dims size is a dot product
between the strides and the input address resulting in a linear local address.  Given a
linear local address we can perform the inverse operation first ordering the object in
natural stride order (largest to smallest) and then successively dividing and modulating
the address:

```clojure
    (loop [idx 0
           addr addr]
      (if (< idx n-elems)
        (let [local-stride (.read strides idx)
              shape-idx (quot addr local-stride)
              local-shape (.read shape idx)]
          (if (< shape-idx local-shape)
            (do
              (let [shape-idx (- shape-idx (.read offsets idx))
                    shape-idx (if (< shape-idx 0)
                                (+ shape-idx local-shape)
                                shape-idx)]
                (.append retval-mut (int shape-idx))
                (recur (unchecked-inc idx) (rem addr local-stride))))
            (recur n-elems addr)))
        [(= 0 addr) retval]))
```

Once we have the non-transposed result, if a transpose operator was applied to the global
dimension object we can transpose the result and get the correct dimensions back.


We now define a global linear address space which as the same number of elements as the
shape and is represented by iterating through the entire shape in row-major format.  For
the initial dimension object, the mapping from the global address space to the local
address space is the identity function.  For the transposed object the mapping from
global address space to local address space is slightly more complex but can be achieved
by a successive modulation/divide like operation.  We do need to reverse the shape
and strides and for now assume max-shape is substitutable by shape:

```clojure
(loop [idx (long 0)
       arg (long arg)
       offset (long 0)]
  (if (< idx n-dims)
    (let [next-max (aget rev-max-shape idx)
          next-stride (aget rev-strides idx)
          next-dim (aget rev-shape idx)
          next-off (aget rev-offsets idx)
          shape-idx (rem (+ arg next-off) next-dim)]
      (recur (inc idx)
             (quot arg next-max)
             (+ offset (* next-stride shape-idx))))
    offset))
```

These three operations form the foundation of the necessary basis to map from
n-dimensional tensor space to a linear address space.  The inverse operation
(local->global) is required so that sparse optimizations can apply in tensor space.


Going back to our original set of operations, select of a row can be achieved by simply
offsetting our initial buffer (the sub-buffer operation) and removing the outer initial
or 'larger' dimension resulting in a dimension object of `{:shape [2] :strides [1]}`.
Select of a column can be achieved by offsetting the intial buffer and removing the
'smaller' dimension, resulting in a dimension object of `{:shape [2] :strides [2]}`.  It
is important to note that this changes the `local->global` address space translation
such that not all local address exist in global space thus making the translation itself
sparse.  Select may also take an array as a dimension thus making address generation
dependent on the contents of the array.  This allows arbitrary reordering and is the
n-dimensional equivalent of an indexed-reader.  It forces a secondary lookup in the
global->local case and a linear search in the local->global case but in both
global->local and the inverse are still available.


Reshape can always be achieved by some combination of sub-buffer and simply replacing
the dimension object entirely.


Broadcasting is achieved by the addition of a `max-shape` element to our dimension
object and slight changes to our transformation functions.  For our purposes we know
that the max-shape element has the same number of entries as our shape but each enty is
greater than or equal to the shape and each entry is evenly divided by the respective
shape entry.  This changes the global->local translation adding in modulation operations
where appropriate.  It also changes the local->global translation in that now, a local
address may represent multiple global addresses.


Rotation is achieved by adding or subtracting an appropriate offset to the address during
global->local transation or vise versa respectively.


The final dimension object has at least 4 fields in the worst case: `{:shape :strides
:offsets :max-shape}`.  We can divine from this object the broadcast, rotation, or
transposition operations that happened but we cannot divine the selection operations.
Regardless, using this object in conjunction with a reader we can effectively operate
in a space of N dimensions for any reasonable N.  Note that because we have the inverse
operations we can also implement sparse support throughout our system which as we
talked about earlier can be defined in terms of a sparse set of indexes and an assocated
data vector.


## Using Tensors


```clojure
user> (require '[tech.v2.datatype :as dtype])
:tech.resource.gc Reference thread starting
nil
user> (require '[tech.v2.tensor :as tens])
nil
user> (def first-tensor (tens/->tensor (->> (range 9)
                                            (partition 3))))
#'user/first-tensor
user> first-tensor
{:buffer
 {:datatype :float64, :backing-store [0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0]},
 :dimensions
 {:shape [3 3],
  :strides [3 1],
  :offsets [0 0],
  :max-shape [3 3],
  :global->local
  #<dimensions$create_dimension_transforms$reify__48624@71aa7a11:
    #object[tech.v2.tensor.dimensions$get_elem_dims_global$reify__48557 0x439da0d "tech.v2.tensor.dimensions$get_elem_dims_global$reify__48557@439da0d"]>,
  :local->global
  #<dimensions$create_dimension_transforms$reify__48628@49a4c41e:
    #object[tech.v2.tensor.dimensions$get_elem_dims_local$reify__48599 0x7c033cbe "tech.v2.tensor.dimensions$get_elem_dims_local$reify__48599@7c033cbe"]>},
 :buffer-type :tensor}
user> (println first-tensor)
#tech.v2.tensor<float64>[3 3]
[[0.000 1.000 2.000]
 [3.000 4.000 5.000]
 [6.000 7.000 8.000]]
nil
user> (dtype/->vector first-tensor)
[0.0 1.0 2.0 3.0 4.0 5.0 6.0 7.0 8.0]
user> (vec (dtype/->reader-of-type first-tensor :int32))
[0 1 2 3 4 5 6 7 8]
user> (tens/->tensor first-tensor)
Execution error (IllegalArgumentException) at tech.v2.datatype.base/eval34844$fn (base.cljc:256).
Don't know how to create ISeq from: clojure.lang.Keyword
user> (tens/->jvm first-tensor)
[[0.0 1.0 2.0] [3.0 4.0 5.0] [6.0 7.0 8.0]]
user> (def int-data (dtype/copy! first-tensor (int-array 9)))
#'user/int-data
user> int-data
[0, 1, 2, 3, 4, 5, 6, 7, 8]
user> (println (tens/transpose first-tensor [1 0]))
#tech.v2.tensor<float64>[3 3]
[[0.000 3.000 6.000]
 [1.000 4.000 7.000]
 [2.000 5.000 8.000]]
 nil
user> (println (tens/broadcast first-tensor [6 3]))
#tech.v2.tensor<float64>[6 3]
[[0.000 1.000 2.000]
 [3.000 4.000 5.000]
 [6.000 7.000 8.000]
 [0.000 1.000 2.000]
 [3.000 4.000 5.000]
 [6.000 7.000 8.000]]
nil
user> (println (tens/rotate first-tensor [0 1]))
#tech.v2.tensor<float64>[3 3]
[[2.000 0.000 1.000]
 [5.000 3.000 4.000]
 [8.000 6.000 7.000]]
nil
user> (println (tens/select first-tensor 1 :all))
#tech.v2.tensor<float64>[3]
[3.000 4.000 5.000]
nil
user> (println (tens/select first-tensor :all 2))
#tech.v2.tensor<float64>[3]
[2.000 5.000 8.000]

user> (def sparse-tens (tens/new-tensor [1000 1000] :container-type :sparse))
#'user/sparse-tens
user>  (:buffer sparse-tens)
{:b-offset 0,
 :b-elem-count 1000000,
 :sparse-value 0.0,
 :indexes [],
 :data [],
 :buffer-datatype :float64}
```


## Conclusion


We present a simple extension and demonstrate some simple operations on dense and
sparse data.  The index space operations are in-place and work on an arbitrary
number of dimensions.  In this way, extending our base systems which already support
dense and sparse buffers we can further support dense and sparse n-dimensional tensors.


A subset of the addressing scheme is implementable on gpu environments with or without
more advanced compilation mechanisms.  In fact, it is the scheme used by TVM and Halide
for their buffer descriptions so it presents a very natural translation into those
environments.


We can create readers and writers out of almost anything and thus we can use this
indexing scheme with arbitrary data models not restricted to simple dense buffers of
data.  In that way we can project arbitrary data and build arbitrary algebras out of a
relatively simple hierarchy of abstractions.
