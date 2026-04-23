# HOST Migration Engine — Documento de Contexto

## Visão Geral
Projeto de migração do sistema **HOST** (Firebird) para o banco de destino **lc_sistemas** (MySQL).

**Diferença principal em relação ao gdoor-migration:**  
- **Origem**: Firebird  `host` ( Firebird)  
- **Destino**: MySQL schema `lc_sistemas`  
- Sem dependência de Firebird/Jaybird — apenas `mysql-connector-j`

---

## Estrutura de Pacotes

```
src/br/com/lcsistemas/host/
├── Main.java                    # Ponto de entrada
├── config/
│   └── MigracaoConfig.java      # Config origem+destino (ambos MySQL)
├── core/
│   ├── MigracaoContext.java     # Contexto compartilhado entre steps
│   ├── MigracaoException.java   # Exceção da migração
│   └── MigracaoStep.java        # Interface dos steps
├── engine/
│   └── MigracaoEngine.java      # Orquestrador (SwingWorker, sem Firebird)
├── adaptador/
│   ├── AdaptadorMigracao.java   # Interface
│   └── MigracaoHost.java        # Estratégia HOST com lista de 12 steps
├── step/
│   ├── StepBase.java            # Base com helpers JDBC
│   ├── NcmStep.java             # host.produtos → lc_sistemas.ncm
│   ├── CestStep.java            # host.produtos → lc_sistemas.cest
│   ├── CategoriaStep.java       # host.produtos_grupo → lc_sistemas.categoria
│   ├── FabricanteStep.java      # host.produtos_marca → lc_sistemas.fabricante
│   ├── UnidadeStep.java         # host.produtos → lc_sistemas.unidade
│   ├── CstCfopStep.java         # Resolve id_cst/id_cfop em host.produtos
│   ├── SubcategoriaStep.java    # Placeholder (reset tabela destino)
│   ├── ProdutoStep.java         # host.produtos → lc_sistemas.produto
│   ├── FornecedorStep.java      # host.fornecedor → lc_sistemas.fornecedor
│   ├── ClienteStep.java         # host.clientes → lc_sistemas.cliente
│   ├── PagarStep.java           # host.contas_pagar → lc_sistemas.pagar
│   └── ReceberStep.java         # host.contas_receber → lc_sistemas.receber
└── ui/
    └── MainFrame.java           # UI Swing (3 abas: Destino, Origem HOST, Cliente)
```

---

## Tabelas Origem (host.*) → Destino (lc_sistemas.*)

| Tabela Origem              | Tabela Destino              | Step               |
|----------------------------|-----------------------------|--------------------|
| host.produtos              | lc_sistemas.ncm             | NcmStep            |
| host.produtos              | lc_sistemas.cest            | CestStep           |
| host.produtos_grupo        | lc_sistemas.categoria       | CategoriaStep      |
| host.produtos_marca        | lc_sistemas.fabricante      | FabricanteStep     |
| host.produtos              | lc_sistemas.unidade         | UnidadeStep        |
| lc_sistemas.cst + host     | host.produtos.id_cst/id_cfop| CstCfopStep        |
| host.produtos              | lc_sistemas.produto         | ProdutoStep        |
| host.fornecedor            | lc_sistemas.fornecedor      | FornecedorStep     |
| host.clientes              | lc_sistemas.cliente         | ClienteStep        |
| host.contas_pagar          | lc_sistemas.pagar           | PagarStep          |
| host.contas_receber        | lc_sistemas.receber         | ReceberStep        |

---

## Colunas Temporárias Adicionadas e Removidas

As seguintes colunas são **adicionadas na origem** durante a migração e **removidas no cleanup**:

- `host.produtos`: `id_ncm`, `id_cest`, `id_categoria`, `id_fabricante`, `id_unidade`, `id_cst`, `id_cfop`, `id_subcategoria`
- `host.clientes`: `id_cidade`, `id_estado`
- `host.fornecedor`: `id_cidade`, `id_estado`
- `host.contas_receber`: `id_clientes`
- `host.contas_pagar`: `id_fornecedores`

A coluna `codigo` em `lc_sistemas.fornecedor` é adicionada pelo FornecedorStep e **removida no cleanup** pelo PagarStep.

---

## Ordem de Execução (Dependências)

```
1. NcmStep          ← não depende de nada
2. CestStep         ← não depende de nada
3. CategoriaStep    ← usa host.produtos_grupo
4. FabricanteStep   ← usa host.produtos_marca
5. UnidadeStep      ← usa host.produtos.unidade_comecial
6. CstCfopStep      ← usa lc_sistemas.cst + host.produtos
7. SubcategoriaStep ← placeholder
8. ProdutoStep      ← depende de todos os steps acima
9. FornecedorStep   ← usa host.fornecedor (cidade via codigo_municipio)
10. ClienteStep      ← usa host.clientes (cidade via codigo_municipio)
11. PagarStep        ← depende de FornecedorStep (coluna 'codigo')
12. ReceberStep      ← depende de ClienteStep (numero_cartao)
```

---

## Particularidades do Script HOST

### NCM
- Correção de NCM: `replace(replace(ncm,',',''),'.','')` aplicado antes da inserção

### CEST
- Apenas CESTs com 7 dígitos e `cest > 0` são migrados

### Categoria / Fabricante
- Lidos de tabelas separadas: `host.produtos_grupo` e `host.produtos_marca`
- JOIN via `host.produtos.grupo` (FK para produtos_grupo.id) e `host.produtos.marca` (FK para produtos_marca.id)

### CST/CFOP
- Regime Simples: usa `csosn` → tabela B da lc_sistemas.cst  
  - CFOP 355 para csosn 60 e 500, senão 289
- Regime Normal: usa `cst` (normalizado de 3 para 2 dígitos) → tabela A  
  - CFOP 355 para csosn 60/500/560/260/160, senão 289

### Produto
- Código: `id_produto`
- Barras: `IF(length(barras)>=1, barras, barras_cx)`
- Referência: `IF(length(referencia)>=1, referencia, IF(referencia=gtin,'',gtin))`
- Correções pós-inserção: remove barras=referencia, barras=codigo, referencia=codigo

### Cliente
- Cidade/Estado resolvidos via `codigo_municipio` → `lc_sistemas.cidades.codigocidade`
- Fallback: cidade/estado padrão configurado na UI
- obs: concatena FONE + CELULAR + CELULAR2 + "MIGRAÇÃO HOST"
- IE/RG: separados pelo tamanho do CPF/CNPJ

### Fornecedor
- Coluna auxiliar `codigo` adicionada em `lc_sistemas.fornecedor` para mapeamento pelo PagarStep
- Cidade/Estado via `codigo_municipio`

### Contas a Pagar
- Filtro: `situacao = 'ABERTO'` e `id_fornecedores IS NOT NULL`
- Valor: `vlr_conta - IF(vlr_quitacao IS NULL, 0, vlr_quitacao)`

### Contas a Receber
- Filtro: `situacao = 'ABERTO'` e `id_clientes IS NOT NULL` e `saldo > 0`
- Valor: `vlr_conta - IFNULL(vlr_quitacao, 0.0)`

---

## IDs Fixos de Destino (StepBase)

| Constante            | Valor | Descrição                    |
|----------------------|-------|------------------------------|
| ID_EMPRESA           | 1     | Empresa padrão              |
| ID_USUARIO           | 1     | Usuário padrão              |
| ID_PAIS              | 34    | Brasil                      |
| ID_PAGAMENTO_REC     | 6     | Forma pagamento receber     |
| ID_PLANOCONTAS_REC   | 28    | Plano de contas receber     |
| ID_PAGAMENTO_PAGAR   | 1     | Forma pagamento pagar       |
| ID_PLANOCONTAS_PAGAR | 38    | Plano de contas pagar       |
| ID_CONTAMOV          | 1     | Conta movimento padrão      |
| ID_CONVENIO          | 0     | Sem convênio                |

---

## Dependências (lib/)

Apenas MySQL — sem Jaybird/Firebird:

```
lib/mysql-connector-j-8.0.33.jar  ← obrigatório
lib/logback-classic-1.4.14.jar    ← opcional (logging)
lib/logback-core-1.4.14.jar
lib/slf4j-api-2.0.9.jar
```

FlatLaf (UI):
```
C:\netbeans-28-bin\netbeans\LIBS\flatlaf-3.4.jar
```

> **Nota:** As libs FlatLaf e MySQL estão referenciadas no `project.properties`.
> Copie as libs MySQL para `lib/` antes de abrir no NetBeans.
