<?php

namespace Kafka;

use Illuminate\Queue\Connectors\ConnectorInterface;

class KafkaConnector implements ConnectorInterface
{

    public function connect(array $config)
    {
        $conf = new \RdKafka\Conf();

        $conf->set('bootstrap.servers', $config['bootstrap.servers']);
        $conf->set('security.protocol', $config['security.protocol']);
        $conf->set('sasl.mechanism', $config['sasl.mechanism']);
        $conf->set('sasl.username', $config['sasl.username']);
        $conf->set('sasl.password', $config['sasl.password']);

        $producer = new \RdKafka\Producer($conf);

        $conf->set('group.id', $config['group.id']);
        $conf->set('auto.offset.reset', 'earliest');

        $consumer = new \RdKafka\KafkaConsumer($conf);

        return new KafkaQueue($producer, $consumer);
    }
}
