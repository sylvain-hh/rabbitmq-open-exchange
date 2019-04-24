### Versioning is important :

Beta releases are compatible with all versions of RabbitMQ >= 3.7.7 **and** Erlang >= 21.
Those versions have been tested with RabbitMQ Docker's containers 3.7.9 up to 3.7.14.


#### How to test this exchange ?
````
git clone https://github.com/sylvain-hh/rabbitmq-open-exchange
cd rabbitmq-open-exchange
docker run -d --privileged --net=host --hostname rabbit3714 --name rabbit3714 rabbitmq:3.7.14
docker cp release/rabbitmq_open_exchange-3.7.7-erl21-beta.ez rabbit3714:/plugins/
docker cp release/rabbitmq_management-3.7.14-beta.ez rabbit3714:/plugins/rabbitmq_management-3.7.14.ez
docker exec rabbit3714 rabbitmq-plugins enable rabbitmq_open_exchange rabbitmq_management
````
