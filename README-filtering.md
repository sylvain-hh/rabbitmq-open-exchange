# Filtering messages upstream

Filters are defined by the **exchange's arguments** during its creation : you must take into account all those arguments to prevent inequivalence during other declarations of the same exchange. Filtering is done before message is parsed by the exchange's bindings so that it is processed one time only on the exchange entry.<br/>
<br/>If you don't set any of those arguments, the exchange will not filter any message at all.


### By min and max message payload's size

* `min-payload-size` : the minimum size in bytes of the payload (type is integer) : **beware of UTF-8 chars !**
* `max-payload-size` : guess what :)<br/>
Those ones will forcibly close channel if message's payload size exceed defined thresholds. Use this in a protective way for your consumers towards really bad message producers.<br/>
You also may want to just skip the current exchange without closing channel; in that case you must suffix the argument's key name with `-silent`. In such a case, you may want to define an [Alternate Exchange](https://www.rabbitmq.com/ae.html) too as the next destination hop.



### By max message headers 'size' and depth

Computing the size of headers is not obvious : AMQP defines several datatypes, which in turn RabbitMQ implements in specific Erlang's types and data structures. So, we have to choose some empirical values depending on the arguemnt's types and values found in the headers during parsing to compute its global size :
* `string` : the real number of bytes used (**beware of UTF-8 chars**)
* `any number` (integer, float..) : 4 bytes
* `bool` : 1 byte
* `array` : 10 bytes<br/>
This last type raises another problem because nothing prevent headers having 'infinite' array's depth. So the computation has to take care about maximum size of all values and the maximum depth of arrays. This is done to protect consumers, but also to protect the computation itself !<br/>

The argument's keys to set those thresholds are :
* `max-headers-size` : the maximum bytes used by headers given the above computation (16384 **by default**)
* `max-headers-depth` : the maximum number of embedded levels of arrays in headers (2 **by default**)<br/>
Do note here that **default values are used only in the case you define one key but not the other**.

In all cases, if one of the threshold is exceeded, the channel is forcibly closed.


## Examples

### Message payload and headers sizes :
![](https://sylvain-hh.github.io/rabbitmq-open-exchange/imgs/message_sizes.png)<br/>

Headers size is 41; for example it has the same size of a header's key named "a" containing 4 levels of empty arrays.<br>
Payload size is 7 (yes, the [bicycle UTF-8](https://graphemica.com/%F0%9F%9A%B2) char takes 4 bytes).<br/>

### Exchange arguments :
![](https://sylvain-hh.github.io/rabbitmq-open-exchange/imgs/exchange_filters.png)<br/>

Any message which one of these threshold exceeds will be rejected and channel will be closed :
* headers are not allowed to exceed more than 4 levels of array
* headers size (computed as above) must not exceed 16Kb
* payload size must not exceed 1Kb<br/>

Lastly, messages having a payload size of less than 160 bytes will be silently discarded by this exchange.

