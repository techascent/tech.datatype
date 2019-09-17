(ns tech.v2.datatype.functional.interpreter
  (:require [tech.v2.datatype.functional.impl :as func-impl]))


(set! *unchecked-math* :warn-on-boxed)
(set! *warn-on-reflection* true)



(defonce ^:dynamic *registered-language-fns* (atom {}))


(defn register-symbol!
  [registration-atom sym-name sym-value]
  (-> (swap! (or registration-atom
                 *registered-language-fns*)
             assoc sym-name sym-value)
      keys))


(defn get-operand
  "Return a map of (at least)
  {:type op-type
   :operand op-fn
  }"
  [{:keys [symbol-map]} op-kwd]
  (if-let [item-val (get symbol-map op-kwd)]
    item-val
    (if-let [retval (get @*registered-language-fns* op-kwd)]
      retval
      (throw (ex-info (format "Failed to find math operand: %s" op-kwd)
                      {:operand op-kwd})))))


(defn eval-expr
  "Tiny simple interpreter."
  [env math-expr]
  (cond
    (sequential? math-expr)
    (if (symbol? (first math-expr))
      (let [fn-name (first math-expr)
            ;;Force errors early
            expr-args (mapv (partial eval-expr env) (rest math-expr))
            operand (get-operand env fn-name)]
        (try
          (apply operand env expr-args)
          (catch Throwable e
            (throw (ex-info (format "Operator %s failed:\n%s" math-expr (.getMessage e))
                            {:math-expression math-expr
                             :error e})))))
      (map partial eval-expr env math-expr))
    (symbol? math-expr)
    (get-operand env math-expr)
    :else
    math-expr))


(defn symbol->str
  [sym]
  (if (namespace sym)
    (str (namespace sym) "/" (name sym))
    (str (name sym))))


(defn ignore-env-fn
  [target-fn]
  (fn [_ & args]
    (apply target-fn args)))


(defn register-base-symbols
  [& [registration-atom]]
  (let [registration-atom (or registration-atom *registered-language-fns*)
        builtin-set (set (concat (keys func-impl/all-builtins)
                                 [:indexed-reader
                                  :argsort
                                  :binary-search
                                  :argfilter
                                  :magnitude-squared
                                  :magnitude
                                  :dot-product
                                  :mean
                                  :median
                                  :geometric-mean
                                  :harmonic-mean
                                  :variance
                                  :variance-population
                                  :standard-deviation
                                  :standard-deviation-population
                                  :standard-error
                                  :skewness
                                  :skewness-population
                                  :kurtosis
                                  :kurtosis-population
                                  :pearsons-correlation
                                  :spearmans-correlation
                                  :kendalls-correlation
                                  :fixed-rolling-window]))]
    (doseq [kwd builtin-set]
      (if-let [public-var (resolve (symbol "tech.v2.datatype.functional"
                                           (name kwd)))]
        (swap! registration-atom assoc (symbol (name kwd))
               (ignore-env-fn
                @public-var))
        (throw (ex-info (format "Failed to find var: %s" (str kwd)) {}))))
    (keys @registration-atom)))


(register-base-symbols)
