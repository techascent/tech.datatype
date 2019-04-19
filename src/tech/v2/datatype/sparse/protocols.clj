(ns tech.v2.datatype.sparse.protocols)


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


(defprotocol PToSparse
  (convertible-to-sparse? [item])
  (->sparse [item]))


(extend-type Object
  PToSparse
  (convertible-to-sparse? [item] false))


(defn sparse-convertible?
  [item]
  (convertible-to-sparse? item))


(defn as-sparse
  [item]
  (when (sparse-convertible? item)
    (->sparse item)))


(defn safe-index-seq
  [item]
  (when-let [sparse-item (as-sparse item)]
    (index-seq sparse-item)))
