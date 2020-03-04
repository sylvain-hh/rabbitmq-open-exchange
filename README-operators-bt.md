# Binding types

There are 4 types of binding in this exchange :

* ```any``` : at least one match operator must match
* ```all``` : all match operators must match, and message may have headers not defined in the binding
* ```eq``` : all match operators must match, and message must not have headers not defined in the binding
* ```set N``` : all match operators must match, if message header's key exists in the message

Note that cardinality can be set on ```SET``` binding's type to define the minimal number of match operators to be true on headers only.


