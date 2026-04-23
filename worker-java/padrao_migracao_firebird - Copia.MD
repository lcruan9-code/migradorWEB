# Blueprint: Novo Motor de Migração (Firebird → MySQL)

> **Como usar:** Envie este arquivo + o script SQL do novo sistema (SELECTs da origem + INSERTs no destino) para o agente. Ele tem tudo que precisa para montar o motor completo sem tocar em UI, engine, core ou steps fixos.

---

## 0. O Que NÃO Mexer

As classes abaixo são **boilerplate puro** — copie literalmente, troque só o pacote (`br.com.lcsistemas.NOMEDOSISTEMA`):

| Classe | Papel |
|---|---|
| `core/MigracaoStep.java` | Interface com `prepare / execute / cleanup / rollback` |
| `core/MigracaoContext.java` | Transporta conexões, config e mapas entre steps |
| `core/MigracaoException.java` | Exceção tipada da migração |
| `engine/MigracaoEngine.java` | SwingWorker que abre conexões e chama o adaptador |
| `step/StepBase.java` | Helpers JDBC (`exec`, `execIgnore`, `execUpdate`, `execScalarInt`, `contarInseridos`) |
| `step/AjusteGeralStep.java` | Normaliza nulos/texto no destino — **NUNCA adaptar** |
| `step/GrupoTributacaoStep.java` | Monta grupos tributários no destino — **NUNCA adaptar** |
| `ui/MainFrame.java` | Tela Swing — **não tocar** |

---

## 1. O Que Criar / Adaptar

Para um novo sistema chamado `Hiper`, por exemplo, o pacote base é `br.com.lcsistemas.hiper` e você cria/adapta **apenas** estes arquivos:

```
src/br/com/lcsistemas/hiper/
├── config/
│   └── MigracaoConfig.java          ← adaptar (ver seção 3)
├── adaptador/
│   ├── AdaptadorMigracao.java       ← copiar literal
│   └── MigracaoHiper.java           ← adaptar: só o buildSteps()
└── step/
    ├── StepBase.java                ← copiar literal
    ├── AjusteGeralStep.java         ← copiar literal
    ├── GrupoTributacaoStep.java     ← copiar literal
    ├── NcmStep.java                 ← escrever com o SQL fornecido
    ├── CestStep.java                ← escrever com o SQL fornecido
    ├── CategoriaStep.java           ← escrever com o SQL fornecido
    ├── FabricanteStep.java          ← escrever com o SQL fornecido
    ├── UnidadeStep.java             ← escrever com o SQL fornecido
    ├── CstCfopStep.java             ← escrever com o SQL fornecido
    ├── SubcategoriaStep.java        ← escrever com o SQL fornecido
    ├── ProdutoStep.java             ← escrever com o SQL fornecido
    ├── FornecedorStep.java          ← escrever com o SQL fornecido
    ├── ClienteStep.java             ← escrever com o SQL fornecido
    ├── PagarStep.java               ← escrever com o SQL fornecido
    └── ReceberStep.java             ← escrever com o SQL fornecido
```

---

## 2. Regras de Ouro: Origem Firebird é Intocável

Ao ler do Firebird, respeite sempre estas três restrições:

**2.1 — Origem 100% Read-Only**  
Nunca faça `ALTER TABLE`, `INSERT` ou `DELETE` no banco de origem. O banco do cliente pode não ter permissão DDL. Qualquer dado extra necessário para o processo deve viver como variável Java (`Map`, `int`, `String`), nunca como coluna nova na origem.

**2.2 — Sem Joins Trans-Banco**  
O driver JDBC do Firebird não enxerga a conexão MySQL paralela. Nunca escreva:
```sql
-- ❌ PROIBIDO
UPDATE origem.produto INNER JOIN destino.ncm ON ...
```
Faça a resolução de IDs cruzados **exclusivamente via `Map<>` em Java** carregado antes do loop (ver seção 4, Fase A).

**2.3 — Sem Sintaxe MySQL no Firebird**  
`IF()`, `IFNULL()`, `LENGTH()`, `REPLACE()` são MySQL. No Firebird causam `Token Unknown`.  
Use no Firebird apenas: `CASE WHEN`, `COALESCE`, `CHAR_LENGTH`.  
Ou, preferencialmente, traga o dado cru com `SELECT` minimalista e aplique a lógica no laço Java.

---

## 3. MigracaoConfig — Campos Obrigatórios

Além dos campos de conexão (host, porta, usuário, senha da origem e destino), a config do novo sistema **deve ter obrigatoriamente**:

```java
// Dados do cliente/loja
private String clienteUf       = "";    // UF do estado da loja (ex: "PE", "BA")
private int    cidadeDefaultId = 0;     // ID fallback para cliente.id_cidade
private int    estadoDefaultId = 0;     // ID fallback para cliente.id_estado
private String empresaCnpj     = "";
private int    empresaId       = 1;

public String getClienteUf()            { return clienteUf; }
public void   setClienteUf(String v)    { this.clienteUf = v == null ? "" : v; }
public int    getCidadeDefaultId()      { return cidadeDefaultId; }
public void   setCidadeDefaultId(int v) { this.cidadeDefaultId = v; }
public int    getEstadoDefaultId()      { return estadoDefaultId; }
public void   setEstadoDefaultId(int v) { this.estadoDefaultId = v; }
```

> `clienteUf` é obrigatório para o `GrupoTributacaoStep`. Se vazio, o step usa fallback `"PA"`.  
> `cidadeDefaultId` / `estadoDefaultId` são usados pelo `AjusteGeralStep` para clientes sem cidade/estado resolvido.

---

## 4. Padrão de Escrita dos Steps de Dados

Cada step que lê da origem e insere no destino segue este padrão em 3 fases:

### Fase A — Carregar Mapas do Destino (antes do loop)

```java
// Carrega do MySQL (destino) os IDs já existentes para resolver FKs
Map<String, Integer> mapaNcm = new HashMap<>();
Statement st = ctx.getDestinoConn().createStatement();
ResultSet rs = st.executeQuery("SELECT codigo, id FROM lc_sistemas.ncm");
while (rs.next()) mapaNcm.put(rs.getString(1), rs.getInt(2));
rs.close(); st.close();
ctx.setMapaNcm(mapaNcm); // persiste no contexto para uso em ProdutoStep
```

### Fase B — SELECT Minimalista na Origem (Firebird)

```java
// Só busque o que precisa. Sem functions MySQL. Sem subqueries pesadas.
String sql = "SELECT P.CODIGO, P.DESCRICAO, G.NOME AS GRUPO "
           + "FROM PRODUTOS P LEFT JOIN GRUPOS G ON P.ID_GRUPO = G.ID";
ResultSet rsOrigem = ctx.getOrigemConn().createStatement().executeQuery(sql);
```

### Fase C — Loop de Transformação e Insert em Lote

```java
PreparedStatement ps = ctx.getDestinoConn().prepareStatement(
    "INSERT INTO lc_sistemas.produto (id_empresa, nome, id_categoria, ...) VALUES (?,?,?,...)");

while (rsOrigem.next()) {
    // 1. Coleta crua
    String nome   = rsOrigem.getString("DESCRICAO");
    String grupo  = rsOrigem.getString("GRUPO");

    // 2. Transformações (IF/REPLACE/null-safe) em Java
    if (nome == null || nome.trim().isEmpty()) nome = "SEM NOME";
    nome = nome.trim().toUpperCase();

    // 3. Resolve FK via mapa (fallback seguro)
    int idCategoria = mapaCategoria.getOrDefault(grupo, 1);

    // 4. Monta insert em lote
    ps.setInt(1, ID_EMPRESA);
    ps.setString(2, nome);
    ps.setInt(3, idCategoria);
    ps.addBatch();
    inseridos++;
}
ps.executeBatch();
contarInseridos(ctx, inseridos);
```

### Contrato do MigracaoStep (interface)

```java
String getNome();                           // nome exibido no log/progresso
void prepare(MigracaoContext ctx)           // trunca tabela, dropa temp, etc.
    throws MigracaoException;
void execute(MigracaoContext ctx)           // lógica principal
    throws MigracaoException;
void cleanup(MigracaoContext ctx);          // sempre roda (finally) — dropa temps
void rollback(MigracaoContext ctx);         // roda só se execute() falhou
```

### Helpers disponíveis no StepBase

```java
exec(conn, sql)                             // executa DDL/DML, lança MigracaoException
execIgnore(conn, sql, descricao)            // executa ignorando erro (best-effort)
execUpdate(conn, sql)                       // executa DML, retorna rows afetadas
execScalarInt(conn, sql)                    // SELECT que retorna 1 inteiro
contarInseridos(ctx, n)                     // acumula stats de inserção
contarIgnorados(ctx, n)                     // acumula stats de ignorados
contarErros(ctx, n)                         // acumula stats de erro
```

---

## 5. Steps Fixos de Pós-Migração (Obrigatórios, Sempre os Dois Últimos)

**Regra absoluta:** os dois últimos steps de qualquer motor são sempre, nesta ordem:

```
[N-1] AjusteGeralStep       ← normaliza nulos
[N]   GrupoTributacaoStep   ← monta grupos tributários
```

### AjusteGeralStep

Opera 100% no destino (`lc_sistemas.*`). Não lê da origem. Normaliza:
- **fornecedor**: CPF/CNPJ sem máscara → formata, tipo F/J, nulos → `''`/`0`, UPPER, acentos
- **produto**: limpa EAN/referência com `%SEM%`/`%GTIN%`, zera nulos numéricos, FKs default, flags S/N, CST de PIS/COFINS/IPI, recalcula margens, popula `ajusteestoque` + `estoque` + `estoquesaldo`
- **categoria / subcategoria / fabricante / unidade**: TRIM + UPPER + acentos
- **cliente**: CPF/CNPJ, tipo F/J/E, CEP, dezenas de nulos, fallback `cidadeDefaultId`/`estadoDefaultId`

> Copiar literalmente de `br.com.lcsistemas.host.step.AjusteGeralStep`. Trocar só o `package`.

### GrupoTributacaoStep

Opera 100% no destino (`lc_sistemas.*`). Implementa inline a `NOVOGRUPOTRIB2`:
1. Resolve `id_estado` via `clienteUf` (fallback `"PA"`)
2. Corrige `trib_genero` vazio (2 primeiros dígitos do NCM)
3. INSERT DISTINCT em `grupotributacao` com nome composto `NCM+CST+ICMS+FCP+PIS+COFINS+IPI`
4. Cria temp `TRIBUTACAO`, insere mesmo nome por produto
5. UPDATE `produto.id_grupotributacao` via LEFT JOIN com a temp
6. DROP temp

> Copiar literalmente de `br.com.lcsistemas.host.step.GrupoTributacaoStep`. Trocar só o `package`.

---

## 6. buildSteps() — Ordem Canônica

No adaptador do novo sistema (ex: `MigracaoHiper.java`), o método `buildSteps()` sempre termina assim:

```java
private List<MigracaoStep> buildSteps() {
    return Arrays.<MigracaoStep>asList(
        new NcmStep(),           // 1. tabelas de referência fiscal primeiro
        new CestStep(),          // 2.
        new CategoriaStep(),     // 3.
        new FabricanteStep(),    // 4.
        new UnidadeStep(),       // 5.
        new CstCfopStep(),       // 6. atualiza id_cst/id_cfop nos produtos da origem
        new SubcategoriaStep(),  // 7.
        new ProdutoStep(),       // 8. depende de todos os mapas acima
        new FornecedorStep(),    // 9.
        new ClienteStep(),       // 10.
        new PagarStep(),         // 11.
        new ReceberStep(),       // 12.
        new AjusteGeralStep(),   // 13. ← SEMPRE penúltimo
        new GrupoTributacaoStep()// 14. ← SEMPRE último
    );
}
```

> Steps da origem podem variar conforme o sistema, mas os dois últimos são invariáveis.

---

## 7. Checklist de Criação de Novo Motor

Dado o script SQL do novo sistema (SELECTs + INSERTs):

- [ ] Criar pacote `br.com.lcsistemas.NOMEDOSISTEMA`
- [ ] Copiar literalmente (trocar só `package`): `MigracaoStep`, `MigracaoContext`, `MigracaoException`, `MigracaoEngine`, `StepBase`, `AdaptadorMigracao`, `AjusteGeralStep`, `GrupoTributacaoStep`, `MainFrame`
- [ ] Criar `MigracaoConfig` com os campos obrigatórios da seção 3
- [ ] Criar `MigracaoXXXX.java` (adaptador) com `buildSteps()` conforme seção 6
- [ ] Escrever cada step de dados (seção 4) baseado nos SELECTs/INSERTs do script fornecido
- [ ] Confirmar que `AjusteGeralStep` é o penúltimo e `GrupoTributacaoStep` é o último
- [ ] Confirmar que nenhum step lê nem escreve na origem além de `SELECT`
- [ ] Confirmar que não há sintaxe MySQL dentro de queries executadas no Firebird
