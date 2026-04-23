# Status da Migração SYSPDV — Modo SQL Output

**Data:** 2026-04-19  
**Objetivo:** Migrar banco Firebird (SYSPDV) → H2 in-memory → dump `TabelasParaImportacao.sql`  
**Fluxo:** Portal (localhost:3000) → AppWorker (porta 8080) → MigracaoEngine → H2 → dump SQL

---

## Arquitetura Implementada

```
Portal Next.js (3000)
    ↓ POST /migrar (multipart: syspdv_srv.fdb)
AppWorker (8080)
    ↓ salva .fdb em temp, monta MigracaoConfig
MigracaoEngine
    ↓ H2 in-memory (UUID único por job, schema lc_sistemas)
    ↓ bootstrap: banco_novo.sql (schema + dados de referência)
    ↓ Firebird JDBC → 15 steps → INSERT no H2
    ↓ SqlFileWriter → TabelasParaImportacao.sql (MySQL 5.5.38)
```

---

## Evolução Cronológica dos Erros e Fixes

### Fase 1 — Erros de Compilação (RESOLVIDO)
- **Erro:** 29 erros de compilação
- **Causas:**
  - `SqlMemoryStore.java` não existia → criado como stub com `selectAll()`, `insert()`, `insertStatic()`, `setAutoIncrement()`, `clear()`
  - `MainFrame.java`: import estático inválido `PackingUtils.config` → removido
  - `Main.java`: import `MainFrame` ausente → adicionado
- **Status:** BUILD SUCCESSFUL

### Fase 2 — H2 Driver não encontrado (RESOLVIDO)
- **Erro:** `ERRO NA MIGRAÇÃO: Erro ao iniciar H2: org.h2.Driver`
- **Causa:** `h2-2.1.214.jar` não estava no classpath do `run_worker.bat`
- **Fix:** Adicionado `h2-2.1.214.jar` ao `-cp` em `run_worker.bat` e `build_and_run.bat`

### Fase 3 — Schema `lc_sistemas` não encontrado (RESOLVIDO)
- **Erro:** `Schema "lc_sistemas" not found`
- **Causa:** H2 não reconhece `CREATE DATABASE` como criação de schema
- **Fix:** Adicionado em `MigracaoEngine.java` antes do bootstrap:
  ```java
  destinoConn.createStatement().execute("CREATE SCHEMA IF NOT EXISTS lc_sistemas");
  destinoConn.createStatement().execute("SET SCHEMA lc_sistemas");
  ```

### Fase 4 — Tabela `unidade` não encontrada (EM INVESTIGAÇÃO)
- **Erro:** `Table "unidade" not found; SQL statement: DELETE FROM lc_sistemas.unidade`
- **Diagnóstico:** Bootstrap concluido mas **0 tabelas criadas** no H2
- **Problema raiz:** O `banco_novo.sql` contém `USE lc_sistemas;` que em H2 tenta mudar de catálogo (não de schema), quebrando silenciosamente toda a sessão de criação de tabelas

---

## Estado Atual dos Arquivos

### `MigracaoEngine.java` — H2 com UUID único por job
```java
String h2DbId = UUID.randomUUID().toString().replace("-", "");
String h2Url = "jdbc:h2:mem:" + h2DbId
    + ";MODE=MySQL;DATABASE_TO_UPPER=FALSE;CASE_INSENSITIVE_IDENTIFIERS=TRUE;DB_CLOSE_DELAY=-1";

// autoCommit=true durante bootstrap (cada statement independente)
destinoConn.setAutoCommit(true);
destinoConn.createStatement().execute("CREATE SCHEMA IF NOT EXISTS lc_sistemas");
destinoConn.createStatement().execute("SET SCHEMA lc_sistemas");
// bootstrap → setAutoCommit(false) depois
```

### `SqlFileRunner.java` — h2ify + skip de comandos incompatíveis
**Transformações h2ify aplicadas:**
| MySQL | H2 |
|-------|-----|
| `double(12,4)` | `double` |
| `float(M,D)` | `float` |
| `int(11)`, `tinyint(4)`, etc. | `int`, `tinyint`, etc. |
| `longtext`, `mediumtext` | `clob` |
| `tinytext` | `varchar(255)` |
| `longblob`, `mediumblob` | `blob` |
| `ENGINE=InnoDB` | removido |
| `AUTO_INCREMENT=N` (table-level) | removido |
| `DEFAULT CHARSET=latin1` | removido |
| `CHARACTER SET utf8` | removido |
| `COLLATE utf8_general_ci` | removido |
| `UNSIGNED`, `ZEROFILL` | removido |
| `ROW_FORMAT=...` | removido |
| `COMMENT='...'` | removido |

**Statements ignorados (skip):**
- `USE <schema>`
- `CREATE DATABASE ...`
- `DROP DATABASE ...`
- `SET NAMES ...`
- `SET @variavel = ...`

**Novo Statement por execução** (evita cascade failure no H2)

### `run_worker.bat` — classpath correto
```
...;lib\h2-2.1.214.jar;...;dist\host-migration.jar
```

---

## Próximos Passos de Investigação

O fix do `USE lc_sistemas` foi aplicado mas **ainda resultou em 0 tabelas**. Hipóteses restantes a testar:

### Hipótese A — `SET @...` ocorre dentro de bloco multi-linha
O banco_novo.sql tem `/*!40101 SET @OLD... */;` que começa com `/*!` = `/*` e é corretamente PULADO pelo check `startsWith("/*")`. Mas pode haver `SET @` sem o `/*!` wrapper.

**Verificar:**
```bash
grep -n "^SET @" banco_novo.sql | head -10
```

### Hipótese B — H2 não reconhece a sintaxe de backtick em algum contexto
Testar adicionando `sql.replace('`', '"')` no h2ify para converter identifiers.

### Hipótese C — O `banco_novo.sql` no JAR está diferente do arquivo em disco
Verificar com: `unzip -p dist/host-migration.jar br/com/.../banco_novo.sql | head -5`

### Hipótese D — Algum statement antes do primeiro CREATE TABLE invalida a conexão
Adicionar log imediatamente após `CREATE SCHEMA` e `SET SCHEMA` para confirmar que funcionaram.

### Hipótese E — H2 com `DB_CLOSE_DELAY=-1` tem comportamento diferente
Remover `DB_CLOSE_DELAY=-1` e testar novamente — o UUID já garante unicidade.

### Hipótese F — Warnings do SqlFileRunner não aparecem no portal
Os `LOG.warning()` do SqlFileRunner vão para o Java Logger (stdout/stderr do AppWorker),
**não** para o listener do AppWorker. Redirecionar para o listener ou verificar no terminal
onde o run_worker.bat está aberto para ver as mensagens reais de erro.

---

## Diagnóstico Mais Importante a Fazer Agora

**Verificar o terminal/console do run_worker.bat** — ele mostra os `LOG.warning()` do SqlFileRunner.
Esses logs estão em formato:
```
[Bootstrap] <MENSAGEM DO ERRO H2> | SQL: <primeiros 120 chars do SQL>
```

Isso vai revelar QUAL erro específico o H2 está retornando para cada CREATE TABLE.

---

## Arquivos Modificados Nesta Sessão

| Arquivo | Mudança |
|---------|---------|
| `engine/MigracaoEngine.java` | UUID por job, CREATE SCHEMA, SET SCHEMA, autoCommit=true no bootstrap, diagnóstico pós-bootstrap |
| `sql/SqlFileRunner.java` | h2ify(), skip USE/CREATE DATABASE, novo Statement por SQL |
| `sql/SqlMemoryStore.java` | **CRIADO** — stub vazio (era inexistente) |
| `ui/MainFrame.java` | Remove import inválido, adiciona sqlOutputPath automático |
| `Main.java` | Import MainFrame corrigido |
| `run_worker.bat` | h2-2.1.214.jar no classpath |
| `build_and_run.bat` | h2-2.1.214.jar no classpath de compilação |
