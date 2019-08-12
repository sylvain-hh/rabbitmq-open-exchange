# Binding types

There are 4 types of binding in this exchange :

* ```any N``` : at least N match operator(s) must match
* ```all``` : all match operators must match, and message may have headers not defined in the binding
* ```eq``` : all match operators must match, and message must not have headers not defined in the binding
* ```set N``` : all match operators must match, if message header's key exists in the message

Actually, cardinality can be set on ```ANY``` and ```SET``` binding's type by adding the minimal number of operators to be true at least.


