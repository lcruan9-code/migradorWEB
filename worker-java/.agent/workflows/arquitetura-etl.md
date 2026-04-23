---
name: arquitetura-etl
description: Protocolo rígido de formatação de ETLs Síncronos e Assíncronos de Migração que expurgam Consumo abusivo de RAM. Acione isto ao construir qualquer loop migratório.
argument-hint: [Application Main ou Engine Logic]
allowed-tools: Read, Edit
context: fork
agent: general-purpose
---

O usuário pediu modificações na Pipeline de ETL Base: **$ARGUMENTS**

---

## Protocolo obrigatório

### PASSO 1 — Identificar a Estrutura de Lotes
Evite varreduras monolíticas que enlacem objetos em RAM até o final (`List<Produto> listall = dao.findAll()`). 

### PASSO 2 — Desmembramento Local
Ao formatar os loops for() do Agente:
1. Inicialize a chamada do ResultSet Mapeada progressivamente.
2. Invoque o `.clear()` na array list base OBRIGATORIAMENTE ao final da transferência de cada tabela. Restaure o GC em seu ambiente e não entorpeça e consuma a memória do BD.

### PASSO 3 — Interface de Process (SwingWorker Support)
As Pipelines NÃO devem bloquear Visualizações UI (AWT).
Sempre envelope motores JDBC pesados num `Consumer<String>` propagando status de Steps na GUI via Listeners (`.publish()`).
Não recuse charsets nem altere botões de interface Main no mesmo ato de rodar o SQL.
