(ns tech.sparse.protocols)


(defprotocol PSparse
  (index-seq [item]
    "Return a sequence of records of
:global-index
:data-index")
  (zero-value [item])
  (set-stride [item new-stride]
    "Set the stride and return a new buffer sharing the backing store.")
  (stride [item])
  (find-index [idx-buf target-idx])
  (index-reader [item])
  (readers [item]
    "Return a map containing
{
 :indexes - int32 reader of indexes in global space.
 :data - (optional) reader of data values.
}")
  (iterables [item]
    "Return a map containing:
{
 :indexes int32 iterable if indexes in global space.
 :data iterable of values in global space.
}"))


(defprotocol PSparseData
  (data-reader [item]))


(defprotocol PToSparseReader
  (->sparse-reader [item]
    "Sparse readers implement:
PSparse, PSparseData,
tech.datatype.protocols/PDatatype
tech.datatype.protocols/PToReader
clojure.core.matrix.protocols/PElementCount"))


(defprotocol PSparseMutableIndexBuffer
  ;;Returns an tuple of [found? item-idx] that may point to either the index or where
  ;;the item should be inserted.
  (set-index-values! [item old-data-buf new-sparse-item]
    "idx-buf is sorted values.")
  (insert-index! [item data-idx idx]
"(let [[found? data-idx] (find-index idx-buf idx)]
   (when-not found?
      (insert-index! idx-buf data-idx idx)))")
  (remove-index! [item idx])
  (remove-sequential-indexes! [item idx-iterable]))
