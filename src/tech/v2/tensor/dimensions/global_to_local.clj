(ns tech.v2.tensor.dimensions.global-to-local
  "Given a generic description object, return an interface that can efficiently
  transform indexes in global coordinates mapped to local coordinates."
  (:require [tech.v2.tensor.dimensions.shape :as shape]
            [tech.v2.datatype :as dtype]
            [tech.v2.datatype.functional :as dtype-fn]
            [primitive-math :as pmath]
            [insn.core :as insn]
            [insn.op :as insn-op]
            [insn.clojure :as insn-clj]
            [camel-snake-kebab.core :as csk])
  (:import [tech.v2.datatype LongReader]
           [tech.v2.tensor LongTensorReader]
           [java.util List ArrayList Map HashMap]
           [java.lang.reflect Constructor]
           [java.util.function Function]
           [java.util.concurrent ConcurrentHashMap]))

(set! *unchecked-math* :warn-on-boxed)
(set! *warn-on-reflection* true)


(defn buffer-ecount
  "What is the necessary ecount for a given buffer"
  ^long [{:keys [shape strides]}]
  ;;In this case the length of strides is so small as to make the general
  ;;method fast to just use object methods.
  (let [stride-idx (dtype-fn/argmax {:datatype :object} strides)
        stride-val (dtype/get-value strides stride-idx)
        shape-val (dtype/get-value shape stride-idx)
        shape-val (long
                   (cond
                     (number? shape-val)
                     shape-val
                     (shape/classified-sequence? shape-val)
                     (shape/classified-sequence-max shape-val)
                     :else
                     (apply max (dtype/->reader
                                 shape-val :int32))))]
    (* shape-val (long stride-val))))


(defn any-offsets?
  [{:keys [offsets]}]
  (not (every? #(== 0 (long %)) offsets)))


(defn dims->shape-data
  [{:keys [shape strides max-shape] :as dims}]
  (let [direct? (shape/direct-shape? shape)
        shape-ecount (long (apply * max-shape))
        offsets? (any-offsets? dims)
        dense? (if direct?
                 (= shape-ecount
                    (buffer-ecount dims))
                 false)
        increasing? (if direct?
                      (and (apply >= strides)
                           (not offsets?))
                      false)
        vec-shape (shape/shape->count-vec shape)
        broadcast? (not= vec-shape max-shape)]
    {:direct? direct?
     :dense? dense?
     :increasing? increasing?
     :broadcast? broadcast?
     :offsets? offsets?
     :n-dims (count shape)
     :shape-ecount shape-ecount
     :vec-shape vec-shape}))


(defn shape-data->signature
  [shape-data]
  (dissoc shape-data :shape-ecount :vec-shape))


(defn shape-ary->strides
  "Strides assuming everything is increasing and packed"
  ^longs [^longs shape-vec]
  (let [retval (long-array (count shape-vec))
        n-elems (alength retval)
        n-elems-dec (dec n-elems)]
    (loop [idx n-elems-dec
           last-stride 1]
      (if (>= idx 0)
        (let [next-stride (* last-stride
                             (if (== idx n-elems-dec)
                               1
                               (aget shape-vec (inc idx))))]
          (aset retval idx next-stride)
          (recur (dec idx) next-stride))
        retval))))


(defn find-breaks
  "Attempt to reduce the dimensionality of the shape but do not
  change its elementwise global->local definition.  This is done because
  the running time of elementwise global->local is heavily dependent upon the
  number of dimensions in the shape."
  ([^objects shape ^longs strides
    ^longs max-shape ^longs offsets]
   (let [n-elems (alength shape)
         n-elems-dec (dec n-elems)]
     (loop [idx n-elems-dec
            last-stride 1
            last-break n-elems
            breaks nil]
       (if (>= idx 0)
         (let [last-shape-entry (if (== idx n-elems-dec)
                                  nil
                                  (aget shape (inc idx)))
               last-entry-number? (number? last-shape-entry)
               next-stride (pmath/* last-stride
                                    (if (== idx n-elems-dec)
                                      1
                                      (if last-entry-number?
                                        (long last-shape-entry)
                                        -1)))
               current-offset (aget offsets idx)
               last-offset (if (== idx n-elems-dec)
                             current-offset
                             (aget offsets (inc idx)))
               shape-entry (aget shape idx)
               shape-entry-number? (number? shape-entry)
               next-break
               (if (and (pmath/== (aget strides idx)
                                  next-stride)
                        shape-entry-number?
                        (and last-entry-number?
                             (pmath/== (long last-shape-entry)
                                       (aget max-shape (inc idx))))
                        (== current-offset last-offset)
                        (== last-offset 0))
                 last-break
                 (inc idx))
               breaks (if (== next-break last-break)
                        breaks
                        (conj breaks (range next-break last-break)))]
           (recur (dec idx) (aget strides idx) next-break breaks))
         (conj breaks (range 0 last-break))))))
  ([{:keys [shape strides offsets max-shape] :as dims}]
   (find-breaks (object-array shape)
                (long-array strides)
                (long-array max-shape)
                (long-array offsets))))


(defn- shape-entry->number-or-reader
  [shape-entry]
  (cond
    (number? shape-entry)
    shape-entry
    (shape/classified-sequence? shape-entry)
    (shape/classified-sequence->reader shape-entry)
    :else
    (dtype/->reader shape-entry :int64)))


(defn reduce-dimensionality
  "Make a smaller equivalent shape in terms of row-major addressing
  from the given shape."
  ([{:keys [shape strides offsets max-shape] :as dims}
    offsets?]
   (let [shape (object-array shape)
         strides (long-array strides)
         offsets (long-array offsets)
         max-shape (long-array max-shape)
         breaks (find-breaks shape strides max-shape offsets)
         max-shape (long-array (map
                                #(reduce * (map (fn [idx] (aget max-shape (long idx)))
                                                %))
                                breaks))]
     {:shape (object-array (map #(if (== 1 (count %))
                                   (->
                                    (aget shape (long (first %)))
                                    (shape-entry->number-or-reader))
                                   (apply * (map (fn [idx]
                                                   (aget shape (long idx)))
                                                 %)))
                                breaks))
      :strides (long-array (map
                            #(reduce min (map (fn [idx] (aget strides (long idx)))
                                            %))
                            breaks))
      :offsets (when offsets?
                 (long-array (map
                              #(reduce + (map (fn [idx] (aget offsets (long idx)))
                                              %))
                              breaks)))
      :max-shape max-shape
      :max-shape-strides (shape-ary->strides max-shape)}))
  ([{:keys [shape strides offsets max-shape] :as dims}]
   (reduce-dimensionality dims true)))


(defn- ast-symbol-access
  [ary-name dim-idx]
  {:ary-name ary-name
   :dim-idx dim-idx})


(defn- ast-instance-const-fn-access
  [ary-name dim-idx fn-name]
  {:ary-name ary-name
   :dim-idx dim-idx
   :fn-name fn-name})


(defn- make-symbol
  [symbol-stem dim-idx]
  (ast-symbol-access symbol-stem dim-idx))


(defn elemwise-ast
  [dim-idx direct? offsets? broadcast?
   trivial-stride?
   most-rapidly-changing-index?
   least-rapidly-changing-index?]
  (let [shape (make-symbol :shape dim-idx)
        stride (make-symbol :stride dim-idx)
        offset (make-symbol :offset dim-idx)
        max-shape-stride (make-symbol :max-shape-stride dim-idx)]
    (let [idx (if most-rapidly-changing-index?
                `~'idx
                `(~'quot ~'idx ~max-shape-stride))
          offset-idx (if offsets?
                       `(~'+ ~idx ~offset)
                       `~idx)
          shape-ecount (if direct?
                           `~shape
                           (ast-instance-const-fn-access :shape dim-idx :lsize))
          idx-bcast (if (or offsets? broadcast? (not least-rapidly-changing-index?))
                      `(~'rem ~offset-idx ~shape-ecount)
                      `~offset-idx)
          elem-idx (if direct?
                     `~idx-bcast
                     `(.read ~shape ~idx-bcast))]
      (if trivial-stride?
        `~elem-idx
        `(~'* ~elem-idx ~stride)))))


(defn global->local-ast
  [reduced-dims broadcast?]
  (let [^objects shape (:shape reduced-dims)
        ^longs strides (:strides reduced-dims)
        ^longs offsets (:offsets reduced-dims)
        ^longs max-shape (:max-shape reduced-dims)
        ^longs max-shape-strides (:max-shape-strides reduced-dims)
        n-dims (alength shape)
        direct-vec (mapv number? shape)
        offsets? (boolean offsets)
        trivial-last-stride? (== 1 (aget strides (dec n-dims)))]
    {:signature
     {:n-dims n-dims
      :direct-vec direct-vec
      :offsets? offsets?
      :broadcast? broadcast?
      :trivial-last-stride? trivial-last-stride?}
     :ast
     (if (= n-dims 1)
       (elemwise-ast 0 (direct-vec 0) offsets? broadcast?
                     trivial-last-stride? true true)
       (let [n-dims (long n-dims)
             n-dims-dec (dec n-dims)]
         (->> (range n-dims)
              (map (fn [dim-idx]
                     (let [dim-idx (long dim-idx)
                           least-rapidly-changing-index? (== dim-idx 0)
                           ;;This optimization removes the 'rem' on the most significant
                           ;;dimension.  Valid if we aren't broadcasting
                           most-rapidly-changing-index? (and (not broadcast?)
                                                             (== dim-idx n-dims-dec))
                           trivial-stride? (and most-rapidly-changing-index?
                                                trivial-last-stride?)]
                       (elemwise-ast dim-idx (direct-vec dim-idx) offsets? broadcast?
                                     trivial-stride? most-rapidly-changing-index?
                                     least-rapidly-changing-index?))))
              (apply list '+))))}))


(defn elem-idx->addr-fn
  "High-dimension (>3) fallback."
  [reduced-dims]
  (let [^objects shape (:shape reduced-dims)
        ^longs strides (:strides reduced-dims)
        ^longs offsets (:offsets reduced-dims)
        ^longs max-shape (:max-shape reduced-dims)
        ^longs max-shape-strides (:max-shape-strides reduced-dims)
        n-dims (alength shape)
        n-elems (pmath/* (aget max-shape-strides 0)
                         (aget max-shape 0))]
    (if offsets
      (reify LongReader
        (lsize [rdr] n-elems)
        (read [rdr idx]
          (loop [dim 0
                 result 0]
            (if (< dim n-dims)
              (let [shape-val (aget shape dim)
                    offset (aget offsets idx)
                    idx (pmath//
                         (pmath/+ idx offset)
                         (aget max-shape-strides dim))
                    stride (aget strides dim)
                    local-val (if (number? shape-val)
                                (-> (pmath/rem idx (long shape-val))
                                    (pmath/* stride))
                                (-> (.read ^LongReader shape-val
                                           (pmath/rem idx
                                                      (.lsize ^LongReader shape-val)))
                                    (pmath/* stride)))]
                (recur (pmath/inc dim) (pmath/+ result local-val)))
              result))))
      (reify LongReader
        (lsize [rdr] n-elems)
        (read [rdr idx]
          (loop [dim 0
                 result 0]
            (if (< dim n-dims)
              (let [shape-val (aget shape dim)
                    idx (pmath// idx (aget max-shape-strides dim))
                    stride (aget strides dim)
                    local-val (if (number? shape-val)
                                (-> (pmath/rem idx (long shape-val))
                                    (pmath/* stride))
                                (-> (.read ^LongReader shape-val
                                           (pmath/rem idx
                                                      (.lsize ^LongReader shape-val)))
                                    (pmath/* stride)))]
                (recur (pmath/inc dim) (pmath/+ result local-val)))
              result)))))))


(def constructor-args
  (let [name-map {:stride :strides
                  :max-shape-stride :max-shape-strides
                  :offset :offsets}]
    (->>
     [:shape :stride :offset :max-shape :max-shape-stride]
     (map-indexed (fn [idx argname]
                    ;;inc because arg0 is 'this' object
                    [argname {:arg-idx (inc (long idx))
                              :ary-name (get name-map argname argname)
                              :name (csk/->camelCase (name argname))}]))
     (into {}))))


(defn reduced-dims->constructor-args
  [reduced-dims]
  (->> (vals constructor-args)
       (map (fn [{:keys [ary-name]}]
              (if-let [carg (ary-name reduced-dims)]
                carg
                (when-not (= ary-name :offsets)
                  (throw (Exception. (format "Failed to find constructor argument %s"
                                             ary-name)))))))
       (object-array)))


(defn are-dims-bcast?
  "Fast version of broadcasting check for when we know the types
  the types of shapes and max-shape"
  [dims]
  (let [^objects shape (:shape dims)
        ^longs max-shape (:max-shape dims)
        n-elems (alength shape)]
    (loop [idx 0
           bcast? false]
      (if (and (< idx n-elems)
               (not bcast?))
        (let [shape-entry (aget shape idx)
              shape-ecount (if (number? shape-entry)
                             (long shape-entry)
                             (.lsize ^LongReader shape-entry))]
          (recur (pmath/inc idx)
                 (pmath/== shape-ecount (aget max-shape idx))))
        bcast?))))


(defn reduced-dims->ast-or-reader
  ([reduced-dims broadcast? force-reader-fn?]
   (let [n-reduced-dims (alength ^objects (:shape reduced-dims))]
     (if (and (not force-reader-fn?)
              (<= n-reduced-dims 4))
       (assoc
        (global->local-ast reduced-dims broadcast?)
        :constructor-args (reduced-dims->constructor-args reduced-dims)
        :reduced-dims reduced-dims)
       {:reader (elem-idx->addr-fn reduced-dims)
        :reduced-dims reduced-dims})))
  ([reduced-dims broadcast?]
   (reduced-dims->ast-or-reader reduced-dims broadcast? false))
  ([reduced-dims]
   (let [broadcast? (are-dims-bcast? reduced-dims)]
     (reduced-dims->ast-or-reader reduced-dims broadcast? false))))


(defn dims->global->local-transformation
  [{:keys [shape strides offsets max-shape] :as dims}
   & {:keys [shape-data force-reader-fn?]}]
  (let [shape-data (or shape-data (dims->shape-data dims))
        n-dims (long (:n-dims shape-data))
        n-dims-dec (dec n-dims)
        offsets? (:offsets? shape-data)
        broadcast? (:broadcast? shape-data)
        ;;Always produce the max-shape vector
        reduced-dims (reduce-dimensionality dims offsets?)]
    (reduced-dims->ast-or-reader reduced-dims broadcast? force-reader-fn?)))


(defn bool->str
  ^String [bval]
  (if bval "T" "F"))


(defn direct-vec->str
  ^String [^List dvec]
  (let [builder (StringBuilder.)
        iter (.iterator dvec)]
    (loop [continue? (.hasNext iter)]
      (when continue?
        (let [next-val (.next iter)]
          (.append builder (bool->str next-val))
          (recur (.hasNext iter)))))
    (.toString builder)))


(defn ast-sig->class-name
  [{:keys [signature]}]
  (format "GToL%d%sOff%sBcast%sTrivLastS%s"
          (:n-dims signature)
          (direct-vec->str (:direct-vec signature))
          (bool->str (:offsets? signature))
          (bool->str (:broadcast? signature))
          (bool->str (:trivial-last-stride? signature))))


(defmulti apply-ast-fn!
  (fn [ast ^Map fields ^List instructions]
    (first ast)))


(defn ensure-field!
  [{:keys [ary-name dim-idx] :as field} ^Map fields]
  (-> (.computeIfAbsent
       fields field
       (reify Function
         (apply [this field]
           (assoc field
                  :field-idx (.size fields)
                  :name (if (:fn-name field)
                          (format "%s%d-%s"
                                  (csk/->camelCase (name ary-name))
                                  dim-idx
                                  (csk/->camelCase (name (:fn-name field))))
                          (format "%s%d"
                                  (csk/->camelCase (name ary-name))
                                  dim-idx))))))
      :name))


(defn push-arg!
  [ast fields ^List instructions]
  (cond
    (= 'idx ast)
    (.add instructions [:lload 1])
    (map? ast)
    (do
      (.add instructions [:aload 0])
      (.add instructions [:getfield :this (ensure-field! ast fields) :long]))
    (seq ast)
    (apply-ast-fn! ast fields instructions)
    :else
    (throw (Exception. (format "Unrecognized ast element: %s" ast)))))


(defn reduce-math-op!
  [math-op ast ^Map fields ^List instructions]
  (reduce (fn [prev-arg arg]
            (push-arg! arg fields instructions)
            (when-not (nil? prev-arg)
              (.add instructions [math-op]))
            arg)
          nil
          (rest ast)))


(defmethod apply-ast-fn! '+
  [ast fields instructions]
  (reduce-math-op! :ladd ast fields instructions))


(defmethod apply-ast-fn! '*
  [ast fields instructions]
  (reduce-math-op! :lmul ast fields instructions))


(defmethod apply-ast-fn! 'quot
  [ast fields instructions]
  (reduce-math-op! :ldiv ast fields instructions))


(defmethod apply-ast-fn! 'rem
  [ast fields instructions]
  (reduce-math-op! :lrem ast fields instructions))


(defmethod apply-ast-fn! '.read
  [ast ^Map fields ^List instructions]
  (when-not (= 3 (count ast))
    (throw (Exception. (format "Invalid .read ast: %s" ast))))
  (let [[opname this-obj idx] ast]
    (.add instructions [:aload 0])
    (.add instructions [:getfield :this (ensure-field! this-obj fields) LongReader])
    (push-arg! idx fields instructions)
    (.add instructions [:invokeinterface LongReader "read"])))


(defmethod apply-ast-fn! '.lsize
  [ast ^Map fields ^List instructions]
  (when-not (= 2 (count ast))
    (throw (Exception. (format "Invalid .read ast: %s" ast))))
  (let [[opname this-obj] ast]
    (.add instructions [:aload 0])
    (.add instructions [:getfield :this (ensure-field! this-obj fields) LongReader])
    (.add instructions [:invokeinterface LongReader "lsize"])))


(defmethod apply-ast-fn! 'idx
  [ast ^Map fields ^List instructions]
  (when-not (= 2 (count ast))
    (throw (Exception. (format "Invalid .read ast: %s" ast))))
  (let [[opname this-obj] ast]
    (.add instructions [:aload 0])
    (.add instructions [:getfield :this (ensure-field! this-obj fields) LongReader])
    (.add instructions [:invokeinterface LongReader "lsize"])))



(defn eval-read-ast!
  "Eval the read to isns instructions"
  [ast ^Map fields ^List instructions]
  ;;The ast could be 'idx
  (if (= ast 'idx)
    (push-arg! ast fields instructions)
    (apply-ast-fn! ast fields instructions))
  (.add instructions [:lreturn]))


(defn emit-fields
  [shape-scalar-vec ^Map fields]
  (concat
   (->> (vals fields)
        (sort-by :name)
        (mapv (fn [{:keys [name field-idx ary-name dim-idx fn-name]}]
                (if (and (= ary-name :shape)
                         (not (shape-scalar-vec dim-idx)))
                  (if fn-name
                    {:flags #{:public :final}
                     :name name
                     :type :long}
                    {:flags #{:public :final}
                     :name name
                     :type LongReader})
                  {:flags #{:public :final}
                   :name name
                   :type :long}))))
   [{:flags #{:public :final}
     :name "nElems"
     :type :long}]))


(defn carg-idx
  ^long [argname]
  (if-let [idx-val (get-in constructor-args [argname :arg-idx])]
    (long idx-val)
    (throw (Exception. (format "Unable to find %s in %s"
                               argname
                               (keys constructor-args))))))


(defn load-constructor-arg
  [shape-scalar-vec {:keys [ary-name field-idx name dim-idx fn-name]}]
  (let [carg-idx (carg-idx ary-name)]
    (if (= ary-name :shape)
      (if (not (shape-scalar-vec dim-idx))
        (if fn-name
          [[:aload 0]
           [:aload carg-idx]
           [:ldc (int dim-idx)]
           [:aaload]
           [:checkcast LongReader]
           [:invokeinterface LongReader (clojure.core/name fn-name)]
           [:putfield :this name :long]]
          [[:aload 0]
           [:aload carg-idx]
           [:ldc (int dim-idx)]
           [:aaload]
           [:checkcast LongReader]
           [:putfield :this name LongReader]])
        [[:aload 0]
         [:aload carg-idx]
         [:ldc (int dim-idx)]
         [:aaload]
         [:checkcast Long]
         [:invokevirtual Long "longValue"]
         [:putfield :this name :long]])
      [[:aload 0]
       [:aload carg-idx]
       [:ldc (int dim-idx)]
       [:laload]
       [:putfield :this name :long]])))


(defn emit-constructor
  [shape-type-vec ^Map fields]
  (concat [[:aload 0]
           [:invokespecial :super :init [:void]]]
          (->> (vals fields)
               (sort-by :name)
               (mapcat (partial load-constructor-arg shape-type-vec)))
          [[:aload 0]
           [:aload 4]
           [:ldc (int 0)]
           [:laload]
           [:aload 5]
           [:ldc (int 0)]
           [:laload]
           [:lmul]
           [:putfield :this "nElems" :long]
           [:return]]))


(defn gen-ast-class-def
  [{:keys [signature ast] :as ast-data}]
  (let [cname (ast-sig->class-name ast-data)
        pkg-symbol (symbol (format "tech.v2.datatype.%s" cname))
        ;;map of name->field-data
        fields (HashMap.)
        read-instructions (ArrayList.)
        ;;Which of the shape items are scalars
        ;;they are either scalars or readers.
        shape-scalar-vec (:direct-vec signature)]
    (eval-read-ast! ast fields read-instructions)
    {:name pkg-symbol
     :interfaces [LongReader]
     :fields (vec (emit-fields shape-scalar-vec fields))
     :methods [{:flags #{:public}
                :name :init
                :desc [(Class/forName "[Ljava.lang.Object;")
                       (Class/forName "[J")
                       (Class/forName "[J")
                       (Class/forName "[J")
                       (Class/forName "[J")
                       :void]
                :emit (vec (emit-constructor shape-scalar-vec fields))}
               {:flags #{:public}
                :name "lsize"
                :desc [:long]
                :emit [[:aload 0]
                       [:getfield :this "nElems" :long]
                       [:lreturn]]}
               {:flags #{:public}
                :name "read"
                :desc [:long :long]
                :emit (vec read-instructions)}]}))


(def defined-classes (ConcurrentHashMap.))
(defn get-or-create-reader-fn
  ^LongReader [{:keys [signature] :as ast-data}]
  (.computeIfAbsent ^ConcurrentHashMap defined-classes
                    signature
                    (reify Function
                      (apply [this signature]
                        (try
                          (let [class-def (gen-ast-class-def ast-data)
                                ^Class class-obj (insn/define class-def)
                                ^Constructor first-constructor
                                (first (.getDeclaredConstructors
                                        class-obj))]
                            #(try (.newInstance first-constructor %)
                                  (catch Throwable e
                                    (throw (ex-info "Error instantiating ast object"
                                                    {:error e
                                                     :ast (:ast ast-data)})))))
                          (catch Throwable e
                            (throw (ex-info "Error generating ast object"
                                            {:error e
                                             :ast (:ast ast-data)}))))))))


(defn- ast-data->reader
  [ast-data]
  (if (:reader ast-data)
    (:reader ast-data)
    ((get-or-create-reader-fn ast-data)
     (:constructor-args ast-data))))


(defn dims->global->local-reader
  ^LongReader [dims]
  (-> (dims->global->local-transformation dims)
      (ast-data->reader)))


(defn reduced-dims->global->local-reader
  ^LongReader [reduced-dims]
  (-> (reduced-dims->ast-or-reader reduced-dims)
      (ast-data->reader)))


(defn dims->global->local
  ^LongTensorReader [{:keys [shape stride offset max-shape] :as dims}]
  (let [max-shape (long-array max-shape)
        max-shape-strides (shape-ary->strides max-shape)
        n-dims (alength max-shape-strides)
        n-dims-dec (dec n-dims)
        n-dims-dec-1 (dec n-dims-dec)
        n-dims-dec-2 (dec n-dims-dec-1)
        elemwise-reader (dims->global->local-reader dims)
        n-elems (.lsize elemwise-reader)]
    (reify LongTensorReader
      (lsize [rdr] n-elems)
      (read [rdr idx] (.read elemwise-reader idx))
      (read2d[this row col]
        (let [max-row (aget max-shape n-dims-dec-1)
              max-col (aget max-shape n-dims-dec)]
          (when (or (pmath/>= col max-col)
                    (pmath/>= row max-row))
            (throw (Exception. (format "read2d - Arguments out of range - %s > %s"
                                       [max-row max-col] [row col]))))
          (when-not
              (throw (Exception. (format "Row value %d out of range %d"
                                         col (aget max-shape n-dims-dec-1)))))

          (.read this (pmath/+ (pmath/* row (aget max-shape-strides
                                                  n-dims-dec-1))
                               col))))
      (read3d [this height width chan]
        (.read this (pmath/+
                     (pmath/* height (aget max-shape-strides n-dims-dec-2))
                     (pmath/* width (aget max-shape-strides n-dims-dec-1))
                     chan)))
      (tensorRead [this dims]
        (let [iter (.iterator dims)]
          (.read this (loop [continue? (.hasNext iter)
                             val 0
                             idx 0]
                        (if continue?
                          (let [next-val (long (.next iter))]
                            (recur (.hasNext iter)
                                   (-> (* next-val (aget max-shape-strides idx))
                                       (pmath/+ val))
                                   (pmath/inc idx)))
                          val))))))))


(comment
  (require '[tech.v2.tensor.dimensions :as dims])

  (def test-ast
    (dims->global->local-transformation
     (dims/dimensions [256 [3 2 1 0]]
                      :strides [4 1])))

  (def class-def (gen-ast-class-def test-ast))

  (def class-obj (insn/define class-def))

  (def first-constructor (first (.getDeclaredConstructors class-obj)))

  (def idx-obj (.newInstance first-constructor (:constructor-args test-ast)))

  )
