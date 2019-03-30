(ns tech.sparse.protocols)


(defprotocol PSparse
  (index-seq [item]
    "Get a set of index pairs that tell you what position of the data represents what
destination index.  [data-idx dest-idx]")
  (set-stride [item new-stride]
    "Set the stride and return a new buffer sharing the backing store.")
  (stride [item])
  (->nio-index-buffer [item]
    "If indexes can be represented by a nio buffer, return a nio buffer of the indexes.
This must not return a buffer if stride does not = 1"))


(defprotocol PSparseIndexBuffer
  ;;Returns an integer that may point to either the index or where
  ;;the item should be inserted.
  (find-index [idx-buf target-idx])
  (set-index-values! [item old-data-buf new-idx-buf new-data-buf zero-val]
    "idx-buf is sorted values.")
  (insert-index! [item data-idx idx]
"(let [data-idx (find-index idx-buf idx)]
   (when-not (found-index? idx-buf data-idx idx)
      (insert-index! idx-buf data-idx idx)))")
  (remove-index! [item idx])
  (remove-sequential-indexes! [item data-buffer])
  (offset [item]))


(defprotocol PSparseBuffer
  (set-values! [item new-idx-buf new-data-buf])
  (set-sequential-values! [item new-data-buf]
    "In this case, data buf is all there is.")
  (->sparse-buffer-backing-store [item]
    "If this item is backed by a sparse buffer, return said buffer.  This buffer has a
hardware-level definition of a sorted array of unsigned integers representing indexes
and an equal length array of data.")
  (data-buffer [item]
    "contiguous buffer of data.")
  (->nio-data-buffer [item]
    "If the sparse data backing list can be represented by a nio buffer, return a nio
buffer of the indexes.  This must not return a buffer if stride does not equal 1")
  (index-buffer [item]
    "contiguous buffer of int32 indexes.")
  (zero-value [item]
    "The value that stands for zero in this buffer"))
