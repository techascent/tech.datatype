(ns tech.datatype.functional.impl
  (:require [tech.datatype.unary-op :as unary]
            [tech.datatype.binary-op :as binary]
            [tech.datatype.reduce-op :as reduce-op]
            [tech.datatype.boolean-op :as boolean-op]
            [tech.datatype.iterator :as iterator]
            [tech.datatype.argtypes :as argtypes]
            [tech.datatype.base :as dtype-base]
            [tech.datatype.reader :as reader]
            [tech.datatype.protocols :as dtype-proto]
            [tech.datatype.array]
            [tech.datatype.list]
            [tech.datatype.primitive]))

(def ^:dynamic *datatype* nil)
(def ^:dynamic *unchecked?* nil)

(defn default-options
  [options]
  (merge options
         (when *datatype*
           {:datatype *datatype*})
         (when *unchecked?*
           {:unchecked? *unchecked?*})))


(defonce all-builtins (->> (concat (->> unary/builtin-unary-ops
                                        (map (fn [[k v]]
                                               {:name k
                                                :type :unary
                                                :operator v})))
                                   (->> binary/builtin-binary-ops
                                        (map (fn [[k v]]
                                               {:name k
                                                :type :binary
                                                :operator v})))
                                   (->> boolean-op/builtin-boolean-unary-ops
                                        (map (fn [[k v]]
                                               {:name k
                                                :type :boolean-unary
                                                :operator v})))
                                   (->> boolean-op/builtin-boolean-binary-ops
                                        (map (fn [[k v]]
                                               {:name k
                                                :type :boolean-binary
                                                :operator v}))))
                           (group-by :name)))


(defmacro def-builtin-operator
  [op-name op-seq]
  (let [op-types (->> (map :type op-seq)
                      set)
        op-name-symbol (symbol (name op-name))
        type-map (->> op-seq
                      (map (fn [op-item]
                             [(:type op-item) (:operator op-item)]))
                      (into {}))
        argnum-types (->> op-types
                          (map {:unary :unary
                                :boolean-unary :unary
                                :binary :binary
                                :boolean-binary :binary})
                          set)]
    `(defn ~op-name-symbol
       ~(str "Operator " (name op-name) ":" (vec op-types) "." )
       [& ~'args]
       (let [~'n-args (count ~'args)]
         ~(cond
            (= argnum-types #{:unary :binary})
            `(when-not (> ~'n-args 0)
               (throw (ex-info (format "Operator called with too few (%s) arguments."
                                       ~'n-args))))
            (= argnum-types #{:unary})
            `(when-not (= ~'n-args 1)
               (throw (ex-info (format "Operator takes 1 argument, (%s) given."
                                       ~'n-args))))
            (= argnum-types #{:binary})
            `(when-not (> ~'n-args 1)
               (throw (ex-info (format "Operator called with too few (%s) arguments"
                                       ~'n-args))))
            :else
            (throw (ex-info "Incorrect op types" {:types argnum-types
                                                  :op-types op-types})))
         (let [~'datatype (or *datatype*
                              (dtype-base/get-datatype (first ~'args)))
               ~'options {:datatype ~'datatype
                          :unchecked? *unchecked?*}]
           (if (= ~'n-args 1)
             ~(if (contains? op-types :boolean-unary)
                `(boolean-op/apply-unary-op
                  ~'options
                  (get boolean-op/builtin-boolean-unary-ops
                       ~op-name)
                  (first ~'args))
                `(unary/apply-unary-op
                  ~'options
                  (get unary/builtin-unary-ops ~op-name)
                 (first ~'args)))
             ~(if (contains? op-types :boolean-binary)
                `(apply boolean-op/apply-binary-op
                        ~'options
                        (get boolean-op/builtin-boolean-binary-ops
                             ~op-name)
                        ~'args)
                `(apply binary/apply-binary-op
                        ~'options
                        (get binary/builtin-binary-ops ~op-name)
                        ~'args))))))))


(defmacro define-all-builtins
  []
  `(do
     ~@(->> all-builtins
            (map (fn [[op-name op-seq]]
                   `(def-builtin-operator ~op-name ~op-seq))))))


(defn ->iterable
  [item]
  (if (= :scalar (argtypes/arg->arg-type item))
    (iterator/make-const-iterable item (dtype-base/get-datatype item))
    item))


(defn ->reader
  [item & [datatype]]
  (case (argtypes/arg->arg-type item)
    :scalar
    (reader/make-const-reader item (dtype-base/get-datatype item))
    :iterable
    (dtype-proto/make-container :list
                                (or datatype
                                    (dtype-base/get-datatype item))
                                item {})
    item))
