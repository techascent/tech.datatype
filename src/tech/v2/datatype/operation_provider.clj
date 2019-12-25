(ns tech.v2.datatype.operation-provider
  "The type of objects in an operation decide the nature of the operation.
  This is to allow operator '+' to extend to dates, bignums, and
  complex numbers in a very straighforward way."
  (:require [tech.v2.datatype.base :as base])
  (:refer-clojure :exclude [cast]))


(defmulti half-dispatch-unary-op
  (fn [op lhs args]
    (base/operation-type lhs)))

(defmulti unary-op
  "Perform a unary operation."
  (fn [op lhs args]
    [(base/operation-type lhs) op]))

(defmethod unary-op :default
  [op lhs args]
  (half-dispatch-unary-op op lhs args))

(defmulti half-dispatch-boolean-unary-op
  (fn [op lhs args]
    (base/operation-type lhs)))

(defmulti boolean-unary-op
  "Perform a boolean-unary operation."
  (fn [op lhs args]
    [(base/operation-type lhs) op]))

(defmethod boolean-unary-op :default
  [op lhs args]
  (half-dispatch-boolean-unary-op op lhs args))

(defmulti half-dispatch-binary-op
  (fn [op lhs rhs args]
    [(base/operation-type lhs)
     (base/operation-type rhs)]))

(defmulti binary-op
  "Perform a binary operation"
  (fn [op lhs rhs args]
    [(base/operation-type lhs)
     (base/operation-type rhs)
     op]))

(defmethod binary-op :default
  [op lhs rhs args]
  (half-dispatch-binary-op op lhs rhs args))


(defmulti half-dispatch-boolean-binary-op
  (fn [op lhs rhs args]
    [(base/operation-type lhs)
     (base/operation-type rhs)]))

(defmulti boolean-binary-op
  "Perform a boolean-binary operation"
  (fn [op lhs rhs args]
    [(base/operation-type lhs)
     (base/operation-type rhs)
     op]))

(defmethod boolean-binary-op :default
  [op lhs rhs args]
  (half-dispatch-boolean-binary-op op lhs rhs args))

(defmulti half-dispatch-reduce-op
  (fn [op lhs args]
    (base/operation-type lhs)))

(defmulti reduce-op
  "Perform a reduce operation."
  (fn [op lhs args]
    [(base/operation-type lhs) op]))

(defmethod reduce-op :default
  [op lhs args]
  (half-dispatch-reduce-op op lhs args))


(def base-operation-types [:scalar :iterable :reader])


;; (defn argsort
;;   [values {:keys [parallel?]
;;            :or {parallel? true}
;;            :as options}]
;;   (argsort/argsort values options))


;; (def default-provider
;;   (reify
;;     POperationProvider
;;     (cast [provider lhs options]
;;       (case (:argtype options)
;;         :scalar (casting/cast lhs (:datatype options))
;;         :iterable (base/->iterable lhs (:datatype options) options)
;;         :reader (base/->reader lhs (:datatype options) options)))


;;     (reduce-op [provider lhs op {:keys [datatype] :as options}]
;;       (let [datatype (or datatype (base/get-datatype lhs))
;;             argtype (argtypes/arg->arg-type lhs)
;;             options (assoc options :datatype datatype)
;;             op-kwd op
;;             op
;;             (if (keyword? op)
;;               (if-let [op (get binary-op/builtin-binary-ops op)]
;;                 op
;;                 (throw (Exception. (format "Unable to find boolean op %s" op))))
;;               (dtype-proto/->binary-op op options))]
;;         (cond
;;           (= (:argtype options) :scalar)
;;           lhs
;;           (or (= op-kwd :+)
;;               (= op-kwd :*))
;;           (reduce-op/commutative-reader-reduce options op lhs)
;;           :else
;;           (reduce-op/iterable-reduce-map options op lhs))))))


;; (defmacro define-unary-providers
;;   [operation-type-seq provider]
;;   `(do
;;      ~@(for [op-type operation-type-seq]
;;          `(defmethod unary-provider ~op-type [~'arg] ~provider))))



;; (defmacro define-provider-matrix
;;   [operation-type-seq provider]
;;   `(do
;;      ~@(for [lhs operation-type-seq
;;              rhs operation-type-seq]
;;          `(defmethod binary-provider [~lhs ~rhs] [~'lhs-arg ~'rhs-arg] ~provider))))



;; (define-unary-providers [:scalar :iterable :reader] default-provider)
;; (define-provider-matrix [:scalar :iterable :reader] default-provider)

;; (defmethod apply-fn [:argsort :iterable]
;;   [opname values options]
;;   (argsort/argsort (base/make-container :java-array
;;                                         (base/get-datatype values)
;;                                         values)
;;                    options))

;; (defmethod apply-fn [:argsort :reader]
;;   [opname values options]
;;   (argsort/argsort values options))
