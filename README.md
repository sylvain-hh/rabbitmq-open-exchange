# RabbitMQ Open Exchange : open AMQP data router

````
Currently, this repo and all artifacts are still considered as beta release.
Some work should be done before annoncing the first official release.
````

Especially thanks to its routing capabilities, AMQP is a very good Message Oriented Protocol to be used in a Enterprise System Data Bus. This is even more true when you choose RabbitMQ's implementation thanks to its [protocol extensions](https://www.rabbitmq.com/extensions.html) but yet some simple rules cannot be done easily without the help of custom consumers and/or multiple exchanges that increase complexity in your global Information System.

### Filtering messages by their sizes (content and headers)

You can filter messages upstream (before they are parsed by bindings) based on their payload's size but also on their headers's size; see [README-filtering.md](README-filtering.md) for more details and examples.

### Routing messages with complex rules

Here are some examples to understand what this new exchange can do very easily :

| Snapshot of bindings | Interpretation |
| --- | --- |
| ![](https://sylvain-hh.github.io/rabbitmq-open-exchange/imgs/ex_bindings1.png "Example 1 of bindings") | Consumer 1 wants all messages having a header key 'temp' which value is numeric to go to one of its queue : <br/> &nbsp;&nbsp;&nbsp; - 'cold' when <b>value is &lt;</b> -10<br/> &nbsp;&nbsp;&nbsp; - 'hot' when <b>value is &gt;=</b> 35<br/> &nbsp;&nbsp;&nbsp; - 'normal' when <b>value is between</b> -10 and 35 (excluded).<br/>Consumer 2 wants all messages <b>having a header key named</b> 'temp' which <b>value type is numeric</b> to go to its queue.<br/><br/>Message go to the 'error.temp' queue when it does <b>not</b> have a 'temp' header key or when its value is <b>not</b> numeric. |

You can also route messages depending on more data, in one binding only. Let's look at this one :

| Snapshot of bindings | Interpretation |
| --- | --- |
| ![](https://sylvain-hh.github.io/rabbitmq-open-exchange/imgs/ex_bindings2.png "Example 2 of bindings") | Messages go to the queue 'documents.public' when at least <b>one of these rules</b> is true : <br/> - header key 'document.type' <b>value is 'public' or 'general'</b> <br/> - <b>routing key value</b> is formatted as an AMQP topic of at least 2 words which first is 'document' and second is 'public' case insensitively <br/> - <b>property 'type' value</b> begins with 'doc-pub' case insensitively |

But it can also take some more powerful decisions based on really any data; here is an example :

| Snapshot of bindings | Interpretation |
| --- | --- |
| ![](https://sylvain-hh.github.io/rabbitmq-open-exchange/imgs/ex_bindings3.png "Example 3 of bindings") | 1: do not route any message on saturday or sunday, or if producer is anonymous<br/><br/> 2.1: if routing key matches 'SoMeWoRd' case insensitively (not necessary formatted as an AMQP topic), then route message in the 'log' queue and allow producer to add queues it wants via regex, except all those beginning by 'private.'<br/> 2.2: else, route message only to the 'log.bad' queue |




## How it works.

All routing decisions are driven by the binding's arguments only, exactly like headers exchange does. But contrary to this latter, this one has access to all AMQP data, can take more decisions and has better performances too.<br/>

Behaviours and routing decisions are defined by bindings' arguments only which are splitted in 3 types :
* matching operators : define conditions a binding matches 'true' or 'false' for a given message
* branching operators : let ordering bindings and do some "gotos" or "stops" depending on binding's match
* routing operators : allow to route or unroute message to multiple queues or exchanges at the binding or producer's level depending on binding's match<br/>

By combining the use of regular expressions and/or list of values on some rules and multiple combinations (and/or) of those rules, you can manage powerful routing decisions __at the broker level__.
Please consult the [complete list of currently supported operators](README-operators.md) for a detailed description of them.


## Web management plugin.

The Web Managment Plugin has been customized specifically for this exchange to help you create bindings with direct access to all operators :<br/>

![](https://sylvain-hh.github.io/rabbitmq-open-exchange/imgs/management.png "Customized Web Management plugin")

Not only that, it also list bindings by they defined order.<br/>

#### What is exactly the diff between official versions and those releases ?

Mainly it consists of 4 modifications from priv/www/js/ :
* global.js (for help text only)
* tmpl/add-binding.ejs
* tmpl/bindings.ejs
* tmpl/exchange.ejs


## Some thoughts about this new Exchange.

#### "Open" vs exchange types defined in AMQP

````Headers```` : as long as your current bindings does not define header keys starting by ````-x```` other than ````x-match````, this exchange is 100% compatible.<br/>
````Fanout```` : like Headers type is, this exchange is 100% compatible.<br/>
````Direct```` : use operator ````x-?rk=````.<br/>
````Topic```` : use operator ````x-?rkta=````.<br/>
````default```` : use operator ````x-msg-destq-rk````.<br/>
As you can see, this exchange can emulate all other types defined in AMQP.<br/>

#### Splitting destination queues on per message TTL

Thanks to the ````x-?pr```` operator on 'expiration' property, we can now collect messages having near TTL defined in the same queues, so that those queues don't pill up too much "ghost" messages.

#### New routing pattern based on queue's names

Routing decisions can now be taken on queue's names thanks to the ````x-(msg-|)add(q|e)```` operators. This is new, because in this case consumers can declare Queues only without declaring any bindings. And even more, those decisions can be managed at the binding level or the producer's one.<br/>


## What about performances ?

Broadly speaking, this exchange will perform better than the Headers Exchange (up to 3.7.14 in any case); indeed, it was at the very beginning one of the goal of this new Exchange with adding features. I don't like not commented "numbers" at all, but to give you a simple (maybe bad) idea of its perfs, here is 2 graphs :
<br/>
![](https://sylvain-hh.github.io/rabbitmq-open-exchange/imgs/40b.10o.any.headers.png "Headers Exchange")
<br/>
![](https://sylvain-hh.github.io/rabbitmq-open-exchange/imgs/40b.10o.any.open.png "Open Exchange")
<br/>
(and yes.. the second is Open :)
Those ones have been done with RabbitMQ 3.7.14 with those config files :
<br/>
[40b.10o.any.rabbitmq.config.headers.json](https://sylvain-hh.github.io/rabbitmq-open-exchange/rabbitmq.configFiles/40b.10o.any.rabbitmq.config.headers.json "40b.10o.any.rabbitmq.config.headers.json")
<br/>
[40b.10o.any.rabbitmq.config.open.json](https://sylvain-hh.github.io/rabbitmq-open-exchange/rabbitmq.configFiles/40b.10o.any.rabbitmq.config.open.json "40b.10o.any.rabbitmq.config.open.json")

So, talking about "general performance" is a very hard exercise, i.e. there is a priori no doubt that Topic Exchange is better thanks to its specific data (instead of regex), but that could be covered in more details later..
