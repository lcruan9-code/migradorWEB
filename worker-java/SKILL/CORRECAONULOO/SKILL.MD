---
description: Padronização e Limpeza de Nulos (Correção Nulo)
---

# Skill: Correção de Nulos (CORRECAONULOO)

Esta skill documenta o processo de padronizar dados vazios e nulos no banco MySQL (lc_sistemas) após uma migração de origens não-relacionais ou defasadas, como o GDOOR original.

## O Problema

O GDOOR Firebird frequentemente traz campos numéricos e datas textuais em formatos `null` ou strings de ponto flutuante inválidas (`'0.000'`). Isso quebra a interface do sistema LC que espera tipos formais ou zero.

## Como Aplicar a Limpeza

Ao final da migração, executa-se comandos globais de **UPDATE** sobre todos os registros.
Recomenda-se um script ou procedure que zere campos críticos.

### 1. Limpeza em Produtos

Para impedir furos em relatórios de Estoque (onde o valor estava `null`), as colunas de custo e margem são inicializadas com 0:

```sql
-- Custo Médio e Preços de Venda
UPDATE lc_sistemas.produto SET custo_medio = 0.000 WHERE custo_medio IS NULL;
UPDATE lc_sistemas.produto SET preco_venda = 0.000 WHERE preco_venda IS NULL;
UPDATE lc_sistemas.produto SET preco_venda2 = 0.000 WHERE preco_venda2 IS NULL;

-- Margens de Lucro
UPDATE lc_sistemas.produto SET margem_lucro2 = 0.000 WHERE preco_venda2 = 0.000;
UPDATE lc_sistemas.produto SET desconto_max2 = 0.000 WHERE preco_venda2 = 0.000;

-- Custo/Compra
UPDATE lc_sistemas.produto SET valor_compra = preco_compra WHERE valor_compra IS NULL OR valor_compra = '0.000';
UPDATE lc_sistemas.produto SET preco_compra = preco_custo WHERE preco_compra IS NULL OR preco_compra = '0.000';
```

### 2. Tratando Variáveis de Logradouro (Clientes / Fornecedores)

Normalizar nomes de bairro ou complemento em branco:
```sql
UPDATE lc_sistemas.cliente SET nome = fantasia WHERE nome IS NULL OR nome = '';
UPDATE lc_sistemas.cliente SET bairro = 'CENTRO' WHERE bairro IS NULL OR bairro = '';
UPDATE lc_sistemas.cliente SET complemento = '' WHERE complemento IS NULL;
```

> **Dica Arquitetural**: No GDOOR Migration Engine, esta rotina é empacotada no `AjustePosMigracaoStep`. Contudo, ao usar arquivos GIGANTESCOS (ex: com DUMPS massivos tipo 200MB de INSERTS), as classes Java e leitoras tradicionais `readLine()` podem engasgar por limite delimitado ou bugs com charset legado `ISO-8859-1` do DBeaver. Mantenha os leitores operando através de uma carga integral byte-a-byte repassada inteiramente pela flag oculta `allowMultiQueries=true` do JDBC MySQL nas definições de Connection URI, rodando com o `ps.execute(scriptTotal)` em string!
