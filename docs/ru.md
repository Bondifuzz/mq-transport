# Документация по использованию библиотеки mqtransport

Библиотека *mqtransport* предоставляет интерфейс для коммуникации на основе очередей сообщений. Она располагает логическими абстракциями, благодаря которым пользователь может писать простой и легко читаемый код, не думая о механизмах передачи сообщений, проверке корректности их структуры и т.д. Благодаря этой библиотеке пользователю не нужно изучать API конкретного брокера сообщений. Достаточно указать одного из поддерживаемых брокеров и использовать общий интерфейс для коммуникации.

## Принципы работы mqtransport

В библиотеке есть 5 основных классов - абстракций:
1. *Producer*. Позволяет отправлять сообщения
2. *Consumer*. Позволяет потреблять сообщения
3. *ProducingChannel*. Отвечает за отправку сообщений в очередь. К нему добавляются producer-ы (п.1)
4. *ConsumingChannel*. Отвечает за получение сообщений из очереди. К нему добавляются consumer-ы (п.2)
5. *MQApp*. Главный класс приложения. Отвечает проверку соединения с брокером и управляет каналами (п.3, п.4)

Инициализация приложения проходит следующим образом:
1. Происходит создание объекта *MQApp*.
2. Через через интерфейс *MQApp* создаются и регистрируются входные и выходные каналы к очередям сообщений
3. К созданным каналам добавляются producer-ы и consumer-ы

После инициализации приложение *MQApp* запускается и начинается процесс обмена сообщениями.

## Архитектура mqtransport

В этом разделе подробно рассмотрены абстракции mqtransport и их функции

### Объект приложения MQApp

Объект приложения MQApp соединяет воедино остальные примитивы и координирует их работу. При инициализации объект MQApp проверяет доступность брокера сообщений, при старте запускает отправку и получение сообщений во всех каналах, а при завершении - прекращает. Кроме этого этот объект хранит state (состояние), которым могут пользоваться producer-ы и consumer-ы для своих нужд.

На рисунке ниже показано, что происходит при старте MQApp. Созданные каналы начинают отправку и получение сообщений. По каналам могут проходить сообщения разных типов. Эти сообщения сначала перемещаются от источника к брокеру сообщений, а затем - к адресату, если он доступен. Важно понимать, что канал - это абстракция, которая позволяет отправлять сообщения в очередь или же получать сообщения оттуда. Поэтому у одной очереди должно быть открыто максимум два канала: на чтение и запись.

![](/assets/png/mqtransport.app.png)

Код инициализации приложения выглядит следующим образом:

```python
class MQAppInitializer:

    _settings: AppSettings
    _app: MQApp

    @property
    def app(self):
        return self._app

    def __init__(self, settings: AppSettings):
        self._settings = settings
        self._app = None

    async def do_init(self):

        self._app = await self._create_mq_app()
        self._app.state = None

        try:
            # Проверка доступности брокера
            await self._app.ping()

            # Создание каналов к очередям сообщений
            await self._configure_channels()

        except:
            # В случае неудачи сделать cleanup
            await self._app.shutdown()
            raise

    async def _create_mq_app(self):

        broker = self._settings.message_queue.broker.lower()
        settings = self._settings.message_queue

        # Для каждого брокера сообщений своя реализация MQApp
        if broker == "sqs":
            return await SQSApp.create(
                settings.username,
                settings.password,
                settings.region,
                settings.url,
            )

        raise ValueError(f"Unsupported message broker: {broker}")

    async def _configure_channels(self):
        ...

async def create_mq_instance():
    settings = load_app_settings()
    initializer = MQAppInitializer(settings)
    await initializer.do_init()
    return initializer.app


async def main():
    settings = load_app_settings()
    mq_app = await create_mq_instance(settings)
    await mq_app.start()
```

### Consuming channel и consumer-ы

Consuming channel является абстракцией, которая позволяет получить входящие сообщения из очереди. Этот класс использует API брокера для получения сообщений. На рисунке ниже показано, как происходит получение сообщений. Когда сообщение получено, он распаковывается, проверяется на корректность, валидируется. После этого сообщение отдаётся consumer-у. Если consumer смог без ошибок обработать сообщение, то оно удаляется из очереди и происходит получение нового сообщения.

![](/assets/png/mqtransport.consume.png)

Код создания канала выглядит следующим образом:

```python
class MC_Basic(Consumer):

    # Имя сообщения, на которое зарегистрирован consumer
    name: str = "messages.rand"

    # Модель сообщения (тело содержит одно целочисленное поле)
    class Model(BaseModel):
        rnd: int

    # Функция-обработчик сообщения
    async def consume(self, msg: Model, mq_app: MQApp):
        self._logger.info("Consumed message: rnd=%d", msg.rnd)

class MQAppInitializer:

    _settings: AppSettings
    _app: MQApp

    ...

    async def _configure_channels(self):

        # Доступные очереди
        queues = self._settings.message_queue.queues

        # Создать канал для получения сообщений
        channel = await self._app.create_consuming_channel(queues.basic)

        # Добавить consumer-а к каналу
        channel.add_consumer(MC_Basic())

```

### Producing channel и producer-ы

Producing channel является абстракцией, которая позволяет отправить исходящие сообщения в очередь. Этот класс использует API брокера для отправки сообщений. На рисунке ниже показано, как происходит отправка сообщений. При отправке сообщение сначала валидируется (в dev-режиме), запаковывается, а затем посылается к брокеру.

![](/assets/png/mqtransport.produce.png)

Код создания канала выглядит следующим образом:

```python
class MP_Basic(Producer):

    # Имя сообщения, на которое зарегистрирован producer
    name: str = "messages.rand"

    # Модель сообщения (тело содержит одно целочисленное поле)
    class Model(BaseModel):
        rnd: int

# В state хранится ссылка на объект producer-а
class MQAppState:
    mp_basic: MP_Basic

class MQAppProduceInitializer:

    _settings: AppSettings
    _app: MQApp

    ...

    async def _configure_channels(self):

        state: MQAppState = self._app.state
        queues = self._settings.message_queue.queues

        # Создать канал для отправки сообщений
        channel = await self._app.create_producing_channel(queues.basic)

        # Добавить producer к каналу и сохранить в state
        mp_basic = MP_Basic()
        channel.add_producer(mp_basic)
        state.mp_basic = mp_basic

# В этой функции производится отправка сообщений
async def producing_loop(mq_app: MQApp):

    state: MQAppState = mq_app.state
    mp_basic = state.mp_basic

    while True:
        rand_num = randint(0, 100)
        await mp_basic.produce(rnd=rand_num)
        mp_basic.logger.info("Producing: rnd=%d", rand_num)
        await asyncio.sleep(1)

```

## Примеры кода

Примеры кода находятся в `/examples`

## Прочее

### Dead letter queue

TODO

### Очередь отложенной отправки сообщений

TODO