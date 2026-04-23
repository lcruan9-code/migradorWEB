---
name: parser-jdbc
description: Guia os agentes IAs a preparar e cegar a conexão MySQL/Firebird para executar massivos scripts DDL ou resguardar o encoding do Brasil isolando Exception "Malformed String". Use quando precisar instanciar conexões de Driver em novos projetos ERP.
argument-hint: [nome do arquivo conector ou passo jdbc]
allowed-tools: Read, Edit
context: fork
agent: general-purpose
---

O usuário está conectando os drivers de banco para: **$ARGUMENTS**

---

## Protocolo obrigatório

### PASSO 1 — Localizar a Classe do Conector JDBC

Localize a montagem da URL do driver:
- `src/**/$ARGUMENTS`

### PASSO 2 — Injetar a Blindagem de Performance e Encoding

Ao redigir / editar a conexão:
#### Para MySQL (Conexão Destino)
É expressamente obrigatório injetar a tag multi-queries e a formatação nativa caso existam rotinas pesadas de `AjustePosMigracaoType`:
- **Adicione:** `?useUnicode=true&characterEncoding=UTF-8&allowMultiQueries=true` ao final da URL.

#### Para Firebird / JAYBIRD (Conexão Origem)
Os bancos clássicos como SGBR e Gdoor usam charsets arcaicos. Se você forçar a leitura do Dialeto como UTF-8, o Agente e o App encaram a fatal `Exception Malformed String...`.
- **Setar:** `encoding=WIN1252` (Ou `characterEncoding=ISO8859_1`)
- Jamais use UTF8 em Jaybird a menos que providamente testado no BD fonte.

### PASSO 3 — Executar e Verificar
Relate a String URL modificada no `notify_user` pedindo teste de compilação. Nunca chame scripts pesados sem constar essas flags.
