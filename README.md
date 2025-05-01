# 🐰 POC RabbitMQ + Python  
**Análise de Desempenho com Criação Dinâmica de Recursos**

## 📌 Visão Geral da POC

Esta é uma **Prova de Conceito (POC)** desenvolvida para explorar o desempenho do **RabbitMQ** em cenários extremos de uso, com foco em:

- **Criação dinâmica de exchanges, filas e bindings**
- **Alta carga de mensagens**
- **Resiliência em falhas**
- **Thread safety**
- **Monitoramento e logs estruturados**

A POC foi construída com **FastAPI**, **Pika**, **Tenacity**, **Circuit Breaker** e **Docker Compose** para facilitar testes e análise de desempenho.

---

## 🚀 Funcionalidades Principais

| Funcionalidade | Detalhe |
|----------------|---------|
| **Fluent Interface** | Encadeamento de operações como `.declare_exchange().declare_queue().bind_queue().publish()` |
| **Criação Dinâmica de Recursos** | Declara exchanges/filas na primeira execução |
| **Reconnection Automática** | Reconecta até 5x com backoff exponencial |
| **Thread Safety** | Canais isolados por thread com `threading.local()` |
| **Structured Logging** | Logs em formato JSON com contexto completo |
| **Prometheus Metrics** | (Futuro) Métricas de sucesso/falha |
| **OpenTelemetry Integration** | (Futuro) Rastreamento distribuído |

## 🧠 Decisões de Design

| Elemento | Detalhe |
|---------|----------|
| **Circuit Breaker** | Evita falhas em cascata após falhas persistentes |
| **Retry com Backoff Exponencial** | Trata falhas transitórias de rede |
| **Canais por Thread** | Garante thread-safety e isolamento |
| **Idempotência** | Trata `406 PRECONDITION_FAILED` como sucesso |
| **Confirm Mode** | Garante confirmação de envio (opcional) |
| **Log Estruturado** | Facilita diagnóstico e monitoramento |
| **Validação de Parâmetro** | Evita operações inválidas no RabbitMQ |

## ✅ Boas Práticas Implementadas

| Padrão | Detalhe |
|--------|----------|
| **Resiliência** | Circuit breaker + retry + recovery |
| **Thread Safety** | Canais isolados por thread |
| **Idempotência** | `406 PRECONDITION_FAILED` tratado como sucesso |
| **Log Enriquecido** | Cada log contém contexto relevante |
| **Validação de Entrada** | Evita operações inválidas |
| **Publicação Persistente** | `delivery_mode=2` para mensagens críticas |
| **Tratamento de Erro Específico** | Cada tipo de erro é tratado de forma diferente |
| **Configuração Centralizada** | Todos os parâmetros vêm de `settings.py` |
| **Encapsulamento** | Detalhes internos ocultos do cliente |

---

## 🧱 Estrutura do Projeto

```
project-root/
├── api/                  # Endpoints FastAPI
│   ├── customer/
│   │   ├── models.py     # Modelos Pydantic
│   │   └── routes.py     # Endpoint de criação de cliente
│   └── health/
│       └── routes.py     # Health check do serviço
├── services/             # Lógica de negócios
│   ├── rabbitmq/
│   │   ├── __rabbitmq_service.py  # Conexão e operações básicas
│   │   └── rabbitmq_publisher.py # Interface fluente |
│   └── __init__.py
├── settings.py           # Configuração via .env
├── run.py                # Entry point do FastAPI
├── docker-compose.yml    # RabbitMQ via Docker
├── requirements.txt      # Dependências
└── .env                  # Variáveis de ambiente
```

---

## 🛠️ Requisitos

- Python 3.10+
- Docker e Docker Compose
- Pika, FastAPI, Uvicorn, Tenacity, CircuitBreaker

### Instale as dependências:
```bash
pip install -r requirements.txt
```

---

## 🧪 Como Rodar a POC

1. **Subir o RabbitMQ com Docker Compose**:
```bash
docker-compose up -d --build
```

2. **Executar a API**:
```bash
python run.py
```

3. **Enviar uma requisição de cliente**:
```bash
curl -X POST http://localhost:8080/customer \
  -H "Content-Type: application/json" \
  -d '{"name":"Tiago Tartari","email":"tiago@example.com","uuid":"88c3b2e6-5eab-476b-833d-b839497667b0"}'
```

4. **Acessar o painel do RabbitMQ**:
```
http://localhost:15672
```
---

## 🧾 Objetivo da POC

### 🎯 **Testar limites do RabbitMQ**
- Como o RabbitMQ se comporta sob alta carga com **declaração dinâmica de recursos**
- Qual o impacto de `confirm_mode`, `declare_exchange`, `declare_queue` em cada requisição
- Como garantir resiliência e estabilidade com `tenacity` e `circuitbreaker`

### 🧪 **Cenário de Teste**
- **Declaração de recursos por requisição**
- **Alta concorrência**
- **Mensagens grandes ou pequenas**
- **Ambiente local vs. remoto**

---

## 📋 Configuração Recomendada no `.env`

```env
APP_HOST=0.0.0.0
APP_PORT=8080
LOG_LEVEL=INFO
LOG_FORMAT=json

RABBITMQ_HOST=localhost
RABBITMQ_PORT=5672
RABBITMQ_USER=user
RABBITMQ_PASSWORD=password
RABBITMQ_HEARTBEAT=600
RABBITMQ_BLOCKED_CONNECTION_TIMEOUT=300
RABBITMQ_PUBLISHER_CONFIRM_MODE=True
AUTO_DECLARE_EXCHANGES=True
AUTO_DECLARE_QUEUES=True
```

---

## 🧩 Próximos Passos Recomendados

1. **Worker RabbitMQ (Consumidor)**  
   - Crie um serviço consumidor com interface fluente

2. **Monitoramento com Prometheus (Future)**  
   - Adicione métricas por tipo de evento

3. **Rastreamento com OpenTelemetry (Future)**  
   - Integre com tracing para correlação de logs

4. **Fallback Local (Future)**  
   - Salve mensagens localmente se RabbitMQ estiver offline

5. **Health Check Avançado (Future)**  
   - Verifique se exchanges/filas já existem antes do fluxo

---

## 📦 Estrutura Final de Logs

```json
{
  "timestamp": "2025-04-05T12:34:56.789Z",
  "operation_id": "uuid-gerado",
  "step": "declare_exchange",
  "status": "success",
  "exchange": "customer_events",
  "queue": null,
  "routing_key": null,
  "current_success": true
}
```

---

## 📝 Considerações Finais

| Vantagem | Detalhe |
|----------|----------|
| **Interface fluente** | Fácil leitura e uso |
| **Thread-local channels** | Garante segurança |
| **Confirm delivery** | Garante confiança no envio |
| **Retry e Circuit Breaker** | Aumentam resiliência |
| **Structured Logging** | Facilita diagnóstico |
| **Auto-declaração** | Útil para POCs rápidas |

---

## 🧰 Próximos Passos Recomendados

1. **Worker RabbitMQ**
   - Crie um consumidor com interface fluente

2. **Cache de Recursos**
   - Armazene exchanges/filas já declaradas

3. **Middleware de Logs Otimizado**
   - Use `log_format=text` em produção

4. **Validação de Fluxo**
   - Adicione verificação de pré-requisitos

5. **Reaproveitamento de Canais**
   - Reuse canais entre requisições