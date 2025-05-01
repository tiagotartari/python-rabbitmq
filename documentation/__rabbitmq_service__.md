# 🐰 RabbitMQService - Documentação Técnica

## 📦 Visão Geral

`RabbitMQService` é uma classe que encapsula a conexão, publicação e gerenciamento de recursos do RabbitMQ com foco em **resiliência**, **thread-safety** e **idempotência**.

### 🔧 Principais Dependências
```python
import pika
import pika.exceptions
import logging
import json
from settings import config
from datetime import datetime
from tenacity import retry, stop_after_attempt, wait_exponential, after_log
from circuitbreaker import circuit
from threading import local
```

---

## 🔁 Decorators Aplicados à Classe

### 1. `@circuit(failure_threshold=5, recovery_timeout=60)`
- **O que faz:** Ativa o padrão de *circuit breaker* para evitar falhas em cascata
- **Por quê usar:** 
  - Evita que tentativas repetidas de conexão afetem o sistema como um todo
  - Fornece fallback automático após falhas persistentes
- **Intenção:** Garantir estabilidade em ambientes de alta carga ou instáveis

### 2. `@retry(...)` nos métodos `_connect`, `TryDeclareExchange`, etc.
- **O que faz:** Implementa retentativas com *backoff exponencial*
- **Por quê usar:**
  - Reduz impacto de falhas transitórias de rede
  - Garante recuperação automática em ambientes instáveis
- **Intenção:** Aumentar a confiabilidade em cenários reais

---

## 🧱 Arquitetura Interna

```python
self.config = config.RABBITMQ_PARAMS
self.connection = None
self.channel_local = local()
self.publisher_confirm_mode = self.config['publisher_confirm_mode']
self._connected = False
```

### `channel_local` (threading.local())
- **O que faz:** Cria canais isolados por thread
- **Por quê usar:**
  - O Pika não é *thread-safe*
  - Evita bloqueios e erros `ChannelWrongStateError`
- **Intenção:** Garantir uso seguro em aplicações multithreaded (como FastAPI + Uvicorn)

### `publisher_confirm_mode`
- **O que faz:** Habilita confirm delivery no canal
- **Por quê usar:**
  - Garante que mensagens sejam confirmadas pelo broker
  - Evita perda silenciosa de mensagens em redes instáveis
- **Intenção:** Aumentar confiabilidade em sistemas produtivos

---

## 🔧 Métodos da Classe

### 1. `_connect()`
```python
def _connect(self):
    try:
        # Cria credenciais e parâmetros de conexão
        credentials = pika.PlainCredentials(...)
        parameters = pika.ConnectionParameters(...)
        self.connection = pika.BlockingConnection(parameters)
        self._setup_channel()
        self._connected = True
    except pika.exceptions.AMQPConnectionError as e:
        self._connected = False
    except Exception as e:
        self._connected = False
```

- **O que faz internamente:**
  - Tenta conectar ao RabbitMQ com parâmetros configuráveis
  - Recria canal se já existir e estiver fechado
  - Ativa confirm delivery se configurado
- **Por quê usar:**
  - Garante reconexão automática em falhas transitórias
  - Mantém o serviço funcional mesmo após quedas temporárias de rede
- **Intenção:** Tornar a conexão com RabbitMQ resiliente e autogerenciada

---

### 2. `is_connected()`
```python
def is_connected(self):
    return self.connection and not self.connection.is_closed
```

- **O que faz:** Valida se a conexão ainda está ativa
- **Por quê usar:** Para evitar operações em conexões mortas
- **Intenção:** Garantir que tentativas de uso só ocorram com conexão válida

---

### 3. `_setup_channel()`
```python
def _setup_channel(self):
    if hasattr(self.channel_local, 'channel') and not self.channel_local.channel.is_closed:
        self.channel_local.channel.close()
    self.channel_local.channel = self.connection.channel()
    if self.publisher_confirm_mode:
        self.channel_local.channel.confirm_delivery()
```

- **O que faz internamente:**
  - Fecha canal antigo se ainda estiver aberto
  - Cria novo canal com base na conexão ativa
  - Ativa confirm delivery se necessário
- **Por quê usar:** Garantir canais limpos e funcionais por thread
- **Intenção:** Evitar uso de canais corrompidos ou compartilhados

---

### 4. `get_channel()`
```python
def get_channel(self):
    try:
        if not self.connection or self.connection.is_closed:
            self._connect()
        if not hasattr(self.channel_local, 'channel') or self.channel_local.channel.is_closed:
            self._setup_channel()
        return self.channel_local.channel
    except Exception as e:
        self._connected = False
        return None
```

- **O que faz internamente:**
  - Verifica se conexão está ativa
  - Recria canal se necessário
  - Garante acesso seguro ao canal por thread
- **Por quê usar:** Canal é um recurso frágil e não thread-safe
- **Intenção:** Garantir operações seguras mesmo após falhas

---

### 5. `TryDeclareExchange()`
```python
def TryDeclareExchange(...):
    try:
        self.get_channel().exchange_declare(...)
        return True
    except pika.exceptions.ChannelClosedByBroker as e:
        if e.reply_code == 406: return True
    except pika.exceptions.ChannelError: return False
```

- **O que faz internamente:**
  - Declarar exchanges com tratamento de erro específico
  - Considerar `PRECONDITION_FAILED (406)` como sucesso (idempotência)
- **Por quê usar:** Garantir que exchanges existam sem falhar
- **Intenção:** Permitir uso seguro mesmo com exchanges pré-existentes

---

### 6. `TryDeclareQueue()`
```python
def TryDeclareQueue(...):
    try:
        self.get_channel().queue_declare(...)
        return True
    except pika.exceptions.ChannelClosedByBroker as e:
        if e.reply_code == 406: return True
    except pika.exceptions.ChannelError: return False
```

- **O que faz internamente:**
  - Declarar filas com tratamento de erro específico
  - Considerar `PRECONDITION_FAILED (406)` como sucesso
- **Por quê usar:** Garantir que filas existam sem falhar
- **Intenção:** Permitir uso seguro mesmo com filas pré-existentes

---

### 7. `TryBindQueue()`
```python
def TryBindQueue(...):
    try:
        self.get_channel().queue_bind(...)
        return True
    except pika.exceptions.ChannelClosedByBroker as e:
        logger.error("Broker closed channel during queue binding: %s", str(e))
        return False
```

- **O que faz internamente:**
  - Vincular fila à exchange com tratamento de erro
  - Logar detalhadamente falhas de vinculação
- **Por quê usar:** Garantir que filas possam consumir mensagens
- **Intenção:** Criar fluxos de roteamento confiáveis

---

### 8. `PublishMessage()`
```python
def PublishMessage(...):
    try:
        if not exchange or not isinstance(exchange, str): return False
        if not routing_key or not isinstance(routing_key, str): return False

        body = json.dumps(message, default=str, ensure_ascii=False) 

        self.get_channel().basic_publish(
            exchange=exchange,
            routing_key=routing_key,
            body=body,
            properties=pika.BasicProperties(delivery_mode=2, content_type='application/json')
        )
        return True
    except pika.exceptions.UnroutableError:
        return False
```

- **O que faz internamente:**
  - Valida parâmetros antes do envio
  - Serializa mensagem em JSON
  - Publica mensagem com `delivery_mode=2` (persistente)
- **Por quê usar:** Garantir que mensagens não sejam perdidas
- **Intenção:** Criar um serviço de publicação confiável e seguro

---

## 🧪 Uso Recomendado

```python
from services.rabbitmq.rabbitmq_publisher import RabbitMQPublisher

rabbitmq_publisher = RabbitMQPublisher()

def send_customer_created_event(customer_data):
    result = (
        rabbitmq_publisher()
        .declare_exchange("customer_events", "topic")
        .declare_queue("customer_queue", durable=True)
        .bind_queue("customer_queue", "customer_events", "customer.#")
        .publish("customer_events", f"customer.created.{customer_data.uuid}", customer_data.dict())
        .result()
    )
    rabbitmq_publisher.reset()
    success = bool( rabbitmq_publisher['success'])
    if not result["success"]:
        logger.error("Falha no envio de evento de cliente")
        raise EventProcessingError("Falha ao processar evento")
```

---

## 📊 Benchmark Esperado

| Métrica | Meta |
|--------|-------|
| Tempo médio por mensagem | < 10ms |
| Sucesso com RabbitMQ online | 100% |
| Sucesso após falha | > 95% (com retry) |
| Thread safety | ✅ Garantido com `threading.local()` |
| Reconnection automática | ✅ Após falhas de rede |

---

## ✅ Checklist de Validação

| Item | Status |
|------|--------|
| **Reconnection automática** | ✅ |
| **Canais por Thread** | ✅ |
| **Publicação persistente** | ✅ |
| **Tratamento de `406`** | ✅ |
| **Log estruturado** | ✅ |
| **Validação de entrada** | ✅ |
| **Circuit Breaker** | ✅ |
| **Retry com backoff** | ✅ |
| **Configuração centralizada** | ✅ |

---

## 📝 Considerações Finais

| Melhoria | Detalhe |
|----------|----------|
| **Log de contexto** | Adicionar `operation_id` para rastreabilidade |
| **Validação de corpo** | Evitar mensagens acima de 4MB |
| **Métricas de desempenho** | Adicionar Prometheus counters |
| **Tracing distribuído** | Integração com OpenTelemetry |

