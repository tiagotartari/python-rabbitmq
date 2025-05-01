# 🐰 POC Python RabbitMQ

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

## 📋 Configurações Recomendadas no `.env`

```env
RABBITMQ_HOST=localhost
RABBITMQ_PORT=5672
RABBITMQ_USER=guest
RABBITMQ_PASSWORD=guest
RABBITMQ_HEARTBEAT=600
RABBITMQ_BLOCKED_CONNECTION_TIMEOUT=300
PUBLISHER_CONFIRM_MODE=True
LOG_LEVEL=INFO
```

---


## 🧰 Próximos Passos Recomendados

1. **Interface fluente para consumidores**  
   - Crie um `FluentRabbitMQConsumer` para consumo de mensagens

2. **Validação de mensagem**  
   - Adicione verificação de tamanho da mensagem

3. **Monitoramento com Prometheus**  
   - Contadores de sucesso/falha por tipo de evento

4. **Rastreamento com OpenTelemetry**  
   - Adicione tracing para mensagens de cliente

5. **Health Check Avançado**  
   - Verifique se exchanges/filas existem
