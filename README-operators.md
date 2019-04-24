# List of all currently supported operators

Following tables describe the complete list of currently suported operators : first column is the content of the binding's argument key, the second is the type and content of its value and third is a comment.

#### Convention used for types and value
* ```VALUE``` : any value of any type
* ```NUMBER``` : any number (float, integer..)
  * ```INT``` : any integer
* ```STRING``` : any string
  * ```REGEX``` : a valid PCRE regular expression
  * ```KEY``` : references a message header's key name
  * ```QUEUE``` | ```EXCHANGE``` : references a queue or exchange's name
  * ```PROP``` : an AMQP property's name (`user_id`, `app_id` etc..)
  * ```TOPIC``` : any valid AMQP topic (words separeted by dot char)
* ```(s)``` : implies that value ***may*** be an list of values (restrictions may apply, cf. below)

#### Common prefix : ```x-```
All binding's arguments keys are prefixed by ```x-``` which is defined by AMQP as a reserved prefix name used for future version. That's a good thing as I consider this exchange as an AMQP extension :)

#### No "CC" or "BCC" (not so) funny things
RabbitMQ's AMQP implementation has an unbeliveable feature around CC and BCC header's key called ["Sender-selected Distribution"](https://www.rabbitmq.com/sender-selected.html) that violates multiple aspects of it; technically speaking, this feature permits to deal with multiple routing keys during message routing.<br/>
__Open exchange does not take into account that feature__ : there is always ONE routing key taken into account, that is the only one defined in AMQP protocol.


## Matching operators

### Binding's type
The binding's type determines how the match result is computed (true or false) for any given message; this is done by setting the x-match operator (values are of string type) :
```
x-match                                The binding matches true when..
                       all                .. all operators match true (this is the default behaviour)
                       any                .. at least one operator matches true
```

### Operators
These are ***always prefixed by ``` 'x-?' ```*** followed by
```
hkv                                 Header Key/Value
   = KEY               VALUE(s)           KEY's value is equal to VALUE
   != KEY              VALUE(s)           KEY's value is not equal to VALUE
   re KEY              REGEX(s)           KEY's value matches regex REGEX
   !re KEY             REGEX(s)           KEY's value doesn't match regex REGEX
   < KEY               NUMBER             KEY's numeric value is less than NUMBER
   <= KEY              NUMBER             KEY's numeric value is less than or equal to NUMBER
   >= KEY              NUMBER             KEY's numeric value is more than or equal to NUMBER
   > KEY               NUMBER             KEY's numeric value is more than NUMBER

hk                                  Header Key
  ex                   KEY(s)             KEY exists
  ex(s|n|b)            KEY(s)             KEY exists and its value is of type string | numeric | boolean
  ex!(s|n|b)           KEY(s)             KEY exists and its value is not of type string | numeric | boolean
  !ex                  KEY(s)             KEY does not exist
  
rk                                  Routing Key's value
  =                    STRING(s)          is equal to STRING
  !=                   STRING(s)          is not equal to STRING
  re                   REGEX(s)           matches regex REGEX
  !re                  REGEX(s)           doesn't match regex REGEX
  ta                   TOPIC(s)           matches AMQP topic TOPIC
  !ta                  TOPIC(s)           doesn't match AMQP topic TOPIC
  taci                 TOPIC(s)           matches AMQP topic TOPIC case insensitively
  !taci                TOPIC(s)           doesn't match AMQP topic TOPIC case insensitively

pr                                  Property
  ex                   PROP(s)            PROP has a value
  !ex                  PROP(s)            PROP is not defined
  re PROP              REGEX(s)           PROP's value matches regex REGEX
  !re PROP             REGEX(s)           PROP's value doesn't match regex REGEX
  = PROP               (INT|STRING)(s)    PROP's value is equal
  != PROP              (INT|STRING)(s)    PROP's value is not equal
  < PROP               INT                PROP's int value is less than INT
  <= PROP              INT                PROP's int value is less than or equal to INT
  >= PROP              INT                PROP's int value is more than or equel to INT
  > PROP               INT                PROP's int value is more than INT

dt                                  Current DateTime format
  u                                   Universal
   re                  REGEX              matches regex REGEX
   !re                 REGEX              doesn't match regex REGEX
  l                                   Local
   re                  REGEX              matches regex REGEX
   !re                 REGEX              doesn't match regex REGEX
```

#### List type restrictions :
* ```x-?hkv=``` and ```x-?rk=``` and ```x-?pr=``` cannot use list type when ```x-match``` is ```all```
* ```x-?hkv!=``` and ```x-?rk!=``` and ```x-?pr!=``` cannot use list type when ```x-match``` is ```any```

#### Datetime format returned by the exchange :
String is of the form ````YYYYMMDD HHmmSS d We```` (20 chars) with :
```
YYYYMMDD : YYYY year (4 chars) MM month (01..12) DD day (01..31)
HHmmSS   : HH hour (00..23) mm minute (00..59) SS second (00.59)
d        : day of the week (1 -> monday... 7 -> sunday)
We       : week number as defined in ISO 8601 (01..53)
```

### Using result in other operators
Most of all following operators are suffixed by one of those :
```
-ontrue               Apply when result of binding's match is True
-onfalse              Apply when result of binding's match is False
```


## Routing destinations operators

#### Add or delete routes
Prefixed by
```
x-                   Routes are defined at the binding level
x-msg-               Routes are decided by producers in their messages via its x-(add|del) header keys
```
followed by
```
add                                  Destination is to be added
del                                  Destination is to be deleted
   q                   (QUEUE)             Destination type is queue (whose name is "QUEUE")
   e                   (EXCHANGE)          Destination type is exchange (whose name is "EXCHANGE")
    re                 REGEX                 Destination name(s) whose match a regex
    !re                REGEX                 Destination name(s) whose don't match a regex
```
suffixed by
```
-ontrue                             Apply when binding match result is True
-onfalse                            Apply when binding match result is False
```

When producers are permitted to add/del routes via regex, they are permitted to specify exact destinations names too.

#### Mimic the default exchange
The operator ````x-msg-destq-rk```` tells the binding to use the routing key as a queue name for its destination so that if there is no other defined operator in the binding, its behaviour mimics the default exchange defined by AMQP.

#### Delete main destination
The operator ````x-del-dest```` tells the binding to not take into account the main destination; this is useful in some special situations.



## Branching operators

#### Set binding's order
This operator determines the order of the current binding in which it must be parsed :
```
order               INT            Order of the bindings's flow it will be treated
```
#### Goto and stop

```
goto                INT            The next binding's order to jump to
stop                               Stop the parsing at this current binding
```
suffixed by
```
-ontrue                            Apply when binding match result is True
-onfalse                           Apply when binding match result is False
```
