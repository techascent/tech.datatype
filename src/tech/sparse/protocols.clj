(ns tech.sparse.protocols)


(defprotocol PSparse
  (index-seq [item]
    "Return a sequence of records of
:global-index
:data-index")
  (sparse-value [item])
  (sparse-ecount [item]
    "Number of sparse elements in this datastructure.  Constant time query.")

  (set-stride [item new-stride]
    "Set the stride and return a new buffer sharing the backing store.")
  (stride [item])

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


(defn any-non-sparse?
  [item]
  (boolean (first (index-seq item))))


(defn index-reader
  [item]
  (:indexes (readers item)))


(defn index-iterable
  [item]
  (:indexes (iterables item)))


(defn data-reader
  [item]
  (:data (readers item)))


(defn data-iterable
  [item]
  (:data (iterables item)))


(defprotocol PToSparseReader
  (->sparse-reader [item]
    "Sparse readers implement:
PSparse
tech.datatype.protocols/PDatatype
tech.datatype.protocols/PToReader
clojure.core.matrix.protocols/PElementCount"))


(defn is-sparse?
  [item]
  (or (satisfies? PSparse item)
      (satisfies? PToSparseReader item)))


(defn ->sparse
  [item]
  (cond
    (satisfies? PSparse item) item
    (satisfies? PToSparseReader item) (->sparse-reader item)
    :else
    (throw (ex-info "Item is not sparse" {}))))
