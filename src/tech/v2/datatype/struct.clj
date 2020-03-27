(ns tech.v2.datatype.struct
  "Structs are datatypes composed of primitive datatypes or other structs.
  Similar to records except they do not support string or object columns,
  only numeric values.  They have memset-0 initialization, memcpy copy,
  and defined equals and hash parameters all based on the actual binary
  representation of the data in the struct."
  (:require [tech.v2.datatype.casting :as casting]
            [tech.v2.datatype.base :as dtype-base]
            [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.binary-reader :refer [->binary-reader]]
            [tech.v2.datatype.binary-writer :refer [->binary-writer]]
            [tech.v2.datatype.monotonic-range :as dtype-range]
            [primitive-math :as pmath])
  (:import [tech.v2.datatype BinaryReader BinaryWriter ObjectReader ObjectWriter]
           [java.util.concurrent ConcurrentHashMap]
           [java.util RandomAccess List Map LinkedHashSet Collection]
           [clojure.lang MapEntry]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defonce struct-datatypes (ConcurrentHashMap.))


(defn datatype-size
  ^long [datatype]
  (if-let [struct-dtype (.get ^ConcurrentHashMap struct-datatypes datatype)]
    (long (:datatype-size struct-dtype))
    (-> (casting/datatype->host-type datatype)
        (casting/numeric-byte-width))))


(defn datatype-width
  ^long [datatype]
  (if-let [struct-dtype (.get ^ConcurrentHashMap struct-datatypes datatype)]
    (long (:datatype-width struct-dtype))
    (-> (casting/datatype->host-type datatype)
        (casting/numeric-byte-width))))


(defn widen-offset
  "Ensure the offset starts at the appropriate boundary for the datatype width."
  ^long [^long offset ^long datatype-width]
  (let [rem-result (rem offset datatype-width)]
    (if (== 0 rem-result)
      offset
      (+ offset (- datatype-width rem-result)))))


(defn struct-datatype?
  [datatype]
  (.contains ^ConcurrentHashMap struct-datatypes datatype))


(defn layout-datatypes
  [datatype-seq]
  (let [[datatype-seq widest-datatype current-offset]
        (->> datatype-seq
             (reduce (fn [[datatype-seq
                           widest-datatype
                           current-offset]
                          {:keys [name datatype n-elems] :as entry}]
                       (let [n-elems (long (if n-elems n-elems 1))
                             dtype-width (datatype-width datatype)
                             dtype-size (* (datatype-size datatype) n-elems)
                             dtype-width (min 8 dtype-width)
                             current-offset (long current-offset)
                             widest-datatype (max (long widest-datatype) dtype-width)
                             current-offset (widen-offset current-offset dtype-width)]
                         (when-not name
                           (throw (Exception.
                                   "Datatypes must all be named at this point.")))
                         [(conj datatype-seq (assoc entry
                                                    :offset current-offset
                                                    :n-elems n-elems))
                          widest-datatype
                          (+ current-offset dtype-size)]))
                     [[] 1 0]))
        current-offset (long current-offset)
        widest-datatype (long widest-datatype)
        datatype-size (widen-offset current-offset widest-datatype)]
    {:datatype-size datatype-size
     :datatype-width widest-datatype
     :data-layout datatype-seq
     :layout-map (->> datatype-seq
                      (map (juxt :name identity))
                      (into {}))}))


(defn get-datatype
  [datatype-name]
  (.getOrDefault ^ConcurrentHashMap struct-datatypes
                 datatype-name datatype-name))


(defn offset-of
  "Returns a tuple of [offset dtype]."
  [{:keys [layout-map] :as struct-def} property-vec]
  (if-not (instance? RandomAccess property-vec)
    (if-let [retval (get layout-map property-vec)]
      [(:offset retval) (:datatype retval)]
      (throw (Exception. (format "Property not found: %s" property-vec))))
    (let [^List property-vec (if-not (instance? RandomAccess property-vec)
                               [property-vec]
                               property-vec)
          n-lookup (count property-vec)]
      (loop [idx 0
             n-prop-elems 0
             prop-datatype nil
             struct-def struct-def
             offset 0]
        (if (< idx n-lookup)
          (let [next-val (.get property-vec idx)
                [offset
                 struct-def
                 n-prop-elems
                 prop-datatype]
                (if (number? next-val)
                  (let [next-val (long next-val)]
                    (when-not (< next-val n-prop-elems)
                      (throw (Exception. "Indexed property access out of range")))
                    [(+ offset (* next-val (long (datatype-size prop-datatype))))
                     (get-datatype prop-datatype)
                     0
                     prop-datatype])
                  (if-let [data-val (get-in struct-def [:layout-map next-val])]
                    [(+ offset (long (:offset data-val)))
                     (get-datatype (:datatype data-val))
                     (long (:n-elems data-val))
                     (:datatype data-val)]
                    (throw (Exception.
                            (format "Could not find property %s in %s"
                                    next-val (:datatype-name struct-def))))))]
            (recur (inc idx) (long n-prop-elems) prop-datatype
                   struct-def (long offset)))
          [offset prop-datatype])))))


(defn define-datatype!
  [datatype-name datatype-seq]
  (let [new-datatype (-> (layout-datatypes datatype-seq)
                         (assoc :datatype-name datatype-name))]
    (.put ^ConcurrentHashMap struct-datatypes datatype-name new-datatype)
    new-datatype))


(declare inplace-new-struct)


(deftype Struct [struct-def
                 buffer
                 ^BinaryReader reader
                 ^BinaryWriter writer]
  dtype-proto/PDatatype
  (get-datatype [m] (:datatype-name struct-def))
  dtype-proto/PEndianness
  (endianness [m] (dtype-proto/endianness reader))
  dtype-proto/PClone
  (clone [m datatype]
    (when-not (= datatype (:datatype-name struct-def))
      (throw (Exception. "Invalid datatype")))
    (let [new-buffer (dtype-proto/clone buffer (dtype-proto/get-datatype buffer))]
      (inplace-new-struct (:datatype-name struct-def) new-buffer
                          {:endianness
                           (dtype-proto/endianness reader)})))
  Map
  (size [m] (count (:data-layout struct-def)))
  (containsKey [m k] (.containsKey ^Map (:layout-map struct-def) k))
  (entrySet [m]
    (let [map-entry-data
          (->> (map (comp #(MapEntry. % (.get m %))
                          :name)
                    (:data-layout struct-def)))]
      (LinkedHashSet. ^Collection map-entry-data)))
  (keySet [m] (.keySet ^Map (:layout-map struct-def)))
  (get [m k]
    (when-let [[offset dtype :as _data-vec] (offset-of struct-def k)]
      (if-let [struct-def (.get ^ConcurrentHashMap struct-datatypes dtype)]
        (let [new-buffer (dtype-proto/sub-buffer
                          buffer
                          offset
                          (:datatype-size struct-def))]
          (inplace-new-struct dtype new-buffer
                              {:endianness (dtype-proto/endianness reader)}))
        (let [host-dtype (casting/host-flatten dtype)
              value
              (case host-dtype
                :int8 (.readByte reader offset)
                :int16 (.readShort reader offset)
                :int32 (.readInt reader offset)
                :int64 (.readLong reader offset)
                :float32 (.readFloat reader offset)
                :float64 (.readDouble reader offset))]
          (if (= host-dtype dtype)
            value
            (casting/unchecked-cast value dtype))))))
  (getOrDefault [m k d]
    (or (.get m k) d))
  (put [m k v]
    (when-not writer
      (throw (Exception. "Item is immutable")))
    (if-let [[offset dtype :as _data-vec] (offset-of struct-def k)]
      (if-let [struct-def (.get ^ConcurrentHashMap struct-datatypes dtype)]
        (let [_ (when-not (and (instance? Struct v)
                               (= dtype (dtype-proto/get-datatype v)))

                  (throw (Exception. (format "non-struct or datatype mismatch: %s %s"
                                             dtype (dtype-proto/get-datatype v)))))]
          (dtype-base/copy! (.buffer ^Struct v)
                            (dtype-proto/sub-buffer buffer offset
                                                    (:datatype-size struct-def)))
          nil)
        (let [v (casting/cast v dtype)
              host-dtype (casting/host-flatten dtype)]
          (case host-dtype
            :int8 (.writeByte writer (pmath/byte v) offset)
            :int16 (.writeShort writer (pmath/short v) offset)
            :int32 (.writeInt writer (pmath/int v) offset)
            :int64 (.writeLong writer (pmath/long v) offset)
            :float32 (.writeFloat writer (pmath/float v) offset)
            :float64 (.writeDouble writer (pmath/double v) offset)
            )))
      (throw (Exception. (format "Datatype %s does not containt field %s"
                                 (dtype-proto/get-datatype m)) k)))))


(defn inplace-new-struct
  ([datatype backing-store options]
   (let [struct-def (get-datatype datatype)
         _ (when-not struct-def
             (throw (Exception. (format "Unable to find struct %s" struct-def))))]
     (Struct. struct-def
              backing-store
              (->binary-reader backing-store options)
              (->binary-writer backing-store options))))
  ([datatype backing-store]
   (inplace-new-struct datatype backing-store {})))


(defn new-struct
  ([datatype options]
   (let [struct-def (get-datatype datatype)
         _ (when-not struct-def
             (throw (Exception. (format "Unable to find struct %s" struct-def))))
         ;;binary read/write to nio buffers is faster than our writer-wrapper
         backing-data (dtype-proto/->buffer-backing-store
                       (byte-array (long (:datatype-size struct-def))))]
     (Struct. struct-def
              backing-data
              (->binary-reader backing-data options)
              (->binary-writer backing-data options))))
  ([datatype]
   (new-struct datatype {})))


(defn struct->buffer
  [^Struct struct]
  (.buffer struct))


(defn struct->binary-reader
  ^BinaryReader [^Struct struct]
  (.reader struct))


(defn struct->binary-writer
  ^BinaryWriter [^Struct struct]
  (.writer struct))


(deftype ArrayOfStructs [struct-def
                         ^long elem-size
                         ^long n-elems
                         buffer
                         options]
  ObjectReader
  (getDatatype [rdr] (:datatype-name struct-def))
  (lsize [rdr] n-elems)
  (read [rdr idx]
    (let [sub-buffer (dtype-proto/sub-buffer
                      buffer
                      (* idx elem-size)
                      elem-size)]
      (Struct. struct-def sub-buffer
               (->binary-reader sub-buffer {:endianness endianness})
               (->binary-writer sub-buffer {:endianness endianness})))))


(defn inplace-new-array-of-structs
  ([datatype buffer options]
   (let [struct-def (.get ^ConcurrentHashMap struct-datatypes datatype)
         _ (when-not struct-def
             (throw (Exception. (format "Failed to find datatype %s"
                                        datatype))))
         elem-size (long (:datatype-size struct-def))
         buf-size (dtype-base/ecount buffer)
         _ (when-not (== 0 (rem buf-size elem-size))
             (throw (Exception. "Buffer size is not an even multiple of dtype size.")))
         n-elems (quot buf-size elem-size)]
     (ArrayOfStructs. struct-def
                      elem-size
                      n-elems
                      buffer
                      options)))
  ([datatype buffer]
   (inplace-new-array-of-structs datatype buffer {})))


(defn new-array-of-structs
  [datatype n-elems options]

  )


(defn array-of-structs->columns
  [^ArrayOfStructs structs]

  )





(comment
  (require '[tech.v2.datatype :as dtype])
  (define-datatype! :vec3 [{:name :x :datatype :float32}
                           {:name :y :datatype :float32}
                           {:name :z :datatype :float32}])

  (define-datatype! :segment [{:name :begin :datatype :vec3}
                              {:name :end :datatype :vec3}])

  (def test-vec3 (new-struct :vec3))
  (.put test-vec3 :x 3.0)
  test-vec3
  (def line-segment (new-struct :segment))
  (.put line-segment [:begin :x] 6.0)
  line-segment
  (.put line-segment :end test-vec3)
  (.get line-segment [:end :x])
  (.get line-segment [:end 0])
  )
