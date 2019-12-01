(ns tech.v2.datatype.operation-provider
  "The type of objects in an operation decide the nature of the operation.
  This is to allow operator '+' to extend to dates, bignums, and
  complex numbers in a very straighforward way."
  (:require [tech.v2.datatype.casting :as casting]
            [tech.v2.datatype.base :as base]
            [tech.v2.datatype.unary-op :as unary-op]
            [tech.v2.datatype.binary-op :as binary-op]
            [tech.v2.datatype.boolean-op :as boolean-op]
            [tech.v2.datatype.reduce-op :as reduce-op]
            [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.argtypes :as argtypes]
            [tech.v2.datatype.readers.const :as const-rdr])
  (:refer-clojure :exclude [cast]))


(defprotocol POperationProvider
  "The provider essentially implements each given operation.  The options map
  is guaranteed to contain :argtype which indicates if the operation happens
  in scalar, iterable or reader space."
  (argtype [provider lhs])
  (cast [provider lhs options])
  (unary-op [provider lhs op options])
  (binary-op [provider lhs rhs op options])
  (reduce-op [provider lhs op options]))


(defn operand-type
  "Classify all the base primitive datatypes as numeric types."
  [dtype]
  (if (or (casting/numeric-type? dtype)
          (= :boolean dtype))
    :primitive
    dtype))


(defmulti unary-provider
  "Unary providers switch of the type of the operand."
  operand-type)


(defmulti binary-provider
  "Binary providers are given both datatypes."
  (fn [lhs-dtype rhs-dtype]
    [(operand-type lhs-dtype) (operand-type rhs-dtype)]))


(defn- operation-type
  [options]
  (get options :op-type :default))


;;Implementation details around the default operation provider

(def datatype-width
  (->> [:object :float64 [:int64 :uint64]
        :float32 [:int32 :uint32]
        [:uint16 :int16]
        [:int8 :uint8] :boolean]
       (map-indexed vector)
       (mapcat (fn [[idx entry]]
                 (if (keyword? entry)
                   [[entry idx]]
                   (map vector entry (repeat idx)))))
       (into {})))


(defn next-integer-type
  [lhs rhs]
  (case (max (casting/int-width lhs)
             (casting/int-width rhs))
    8 :int16
    16 :int32
    :int64))


(defn widest-datatype
  [lhs-dtype rhs-dtype]
  (if (= lhs-dtype rhs-dtype)
    lhs-dtype
    (let [lhs-rank (datatype-width lhs-dtype)
          rhs-rank (datatype-width rhs-dtype)]
      (cond
        (< lhs-rank rhs-rank)
        lhs-dtype
        (= lhs-rank rhs-rank)
        (next-integer-type lhs-dtype rhs-dtype)
        :else
        rhs-dtype))))


(def default-provider
  (reify
    POperationProvider
    (argtype [provider lhs] (argtypes/arg->arg-type lhs))
    (cast [provider lhs options]
      (case (:argtype options)
        :scalar (casting/cast lhs (:datatype options))
        :iterable (base/->iterable lhs (:datatype options) options)
        :reader (base/->reader lhs (:datatype options) options)))
    (unary-op [provider lhs op options]
      (let [[op boolean?]
            (if (keyword? op)
              (if-let [un-op (get unary-op/builtin-unary-ops op)]
                [(dtype-proto/->unary-op un-op options) false]
                (if-let [bin-op (get boolean-op/builtin-boolean-unary-ops op)]
                  [(dtype-proto/->unary-boolean-op bin-op options) true]
                  (throw (Exception. (format "Failed to find unary op %s" op)))))
              (if (and (= (operation-type options) :default)
                       (dtype-proto/convertible-to-unary-op? op))
                [(dtype-proto/->unary-op op options) false]
                [(dtype-proto/->unary-boolean-op op options) true]))]
        (if (= (:argtype options) :scalar)
          (op lhs)
          (if boolean?
            (boolean-op/boolean-unary-map options op lhs)
            (unary-op/unary-map options op lhs)))))

    (binary-op [provider lhs rhs op options]
      (let [op-datatype (or (:datatype options)
                            (widest-datatype (base/get-datatype lhs)
                                             (base/get-datatype rhs)))
            options (assoc options :datatype op-datatype)
            [op boolean?]
            (if (keyword? op)
              (if-let [un-op (get binary-op/builtin-binary-ops op)]
                [(dtype-proto/->binary-op un-op options) false]
                (if-let [bin-op (get boolean-op/builtin-boolean-binary-ops op)]
                  [(dtype-proto/->binary-boolean-op bin-op options) true]
                  (throw (Exception. (format "Failed to find binary op %s" op)))))
              (if (and (= (operation-type options) :default)
                       (dtype-proto/convertible-to-binary-op? op))
                [(dtype-proto/->binary-op op options) false]
                [(dtype-proto/->binary-boolean-op op options) true]))]
        (if (= (:argtype options) :scalar)
          (op lhs rhs)
          (let [lhs (if (= (argtypes/arg->arg-type lhs) :scalar)
                      (const-rdr/make-const-reader lhs op-datatype (base/ecount rhs))
                      lhs)
                rhs (if (= (argtypes/arg->arg-type rhs) :scalar)
                      (const-rdr/make-const-reader rhs op-datatype (base/ecount lhs))
                      rhs)]
            (if boolean?
              (boolean-op/boolean-binary-map options op lhs rhs)
              (binary-op/binary-map options op lhs rhs))))))

    (reduce-op [provider lhs op options]
      (let [op-kwd op
            op
            (if (keyword? op)
              (if-let [op (get binary-op/builtin-binary-ops op)]
                op
                (throw (Exception. (format "Unable to find boolean op %s" op))))
              (dtype-proto/->binary-op op options))]
        (cond
          (= (:argtype options) :scalar)
          lhs
          (or (= op-kwd :+)
              (= op-kwd :*))
          (reduce-op/commutative-reader-reduce options op lhs)
          :else
          (reduce-op/iterable-reduce-map options op lhs))))))


(defmethod unary-provider :primitive
  [lhs]
  default-provider)


(defmethod unary-provider :object
  [lhs]
  default-provider)


(defmethod binary-provider [:primitive :primitive]
  [lhs rhs]
  default-provider)

(defmethod binary-provider [:object :primitive]
  [lhs rhs]
  default-provider)

(defmethod binary-provider [:object :object]
  [lhs rhs]
  default-provider)

(defmethod binary-provider [:primitive :object]
  [lhs rhs]
  default-provider)
